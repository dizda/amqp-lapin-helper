#[forbid(unsafe_code)]
#[macro_use]
extern crate tracing;

pub use lapin::{
    message::Delivery, options::*, types::*, BasicProperties, Channel, Connection,
    ConnectionProperties, Consumer as LapinConsumer, ExchangeKind, Queue,
};

pub mod message {
    pub use lapin::message::Delivery;
}

pub mod options {
    pub use lapin::options::*;
}

pub mod types {
    pub use lapin::types::*;
}

use async_trait::async_trait;
use futures_lite::StreamExt;
use lapin::publisher_confirm::PublisherConfirm;
use once_cell::sync::Lazy;
use prometheus::{opts, register_histogram_vec, register_int_gauge_vec, HistogramVec, IntGaugeVec};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_amqp::*;

pub type Requeue = bool;

pub type Result<E> = std::result::Result<E, Error>;
pub type ConsumeResult<E> = std::result::Result<E, Requeue>;

static STAT_CONCURRENT_TASK: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        opts!(
            "amqp_consumer_concurrent_tasks",
            "Current/Max concurrent check",
        ),
        &["exchange_name", "kind"],
    )
    .unwrap()
});

const EXPONENTIAL_SECONDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

static STAT_CONSUMER_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "amqp_consumer_duration",
        "The duration of the consumer",
        &["exchange_name"],
        EXPONENTIAL_SECONDS.to_vec(),
    )
    .unwrap()
});

static STAT_PUBLISHER_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "amqp_publisher_duration",
        "The duration of the publisher",
        &["exchange_name", "routing_key"],
        EXPONENTIAL_SECONDS.to_vec(),
    )
    .unwrap()
});

/// The reconnection delay when it detects an amqp disconnection
const RECONNECTION_DELAY: Duration = Duration::from_millis(1000);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("MpscSendError: {0}")]
    SendError(#[from] SendError<QueueMessage>),
    #[error("acquire-semaphore: {0}")]
    AcquireSemaphore(#[from] AcquireError),
    #[error("AMQP: {0}")]
    Amqp(#[from] lapin::Error),
    #[error("Missing server ID")]
    MissingServerId,
    #[error("String UTF-8 error: {0}")]
    StringUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Bincode: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Consumer: {0}")]
    ConsumerError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Tag an object as Publishable
#[async_trait]
pub trait BrokerPublish {
    fn exchange_name(&self) -> &'static str;
}

/// Plug listeners to the broker.
#[async_trait]
pub trait BrokerListener: Send + Sync {
    /// Bind the queue & struct to this exchange name
    fn exchange_name(&self) -> &'static str;

    /// How to process the Messages queue
    ///  - X: by spawning a task for each of them, up to some concurrent limit X (use semaphore internally)
    fn max_concurrent_tasks(&self) -> usize {
        1
    }

    /// The method that will be called in the struct impl on every messages received
    /// Err(false): reject.requeue = false
    /// Err(true): reject.requeue = true
    async fn consume(&self, delivery: &Delivery) -> std::result::Result<(), bool>;
}

#[async_trait]
pub trait BrokerManager {
    async fn declare_publisher(channel: &Channel) -> Result<()>;
    async fn declare_consumer(channel: &Channel) -> Result<lapin::Consumer>;
}

/// AMQP Client
pub struct Broker<M: BrokerManager> {
    conn: Option<Connection>,
    /// The publisher will be copied & cloned across the app.
    /// Messages will be pushed into a queue, and then process to amqp asynchronously.
    publisher: Publisher,
    /// A daemon is spawned to process messages queue
    publisher_queue: PublisherQueue,
    consumer: Consumer,
    manager: M,
    uri: String,
}

impl<M: BrokerManager> Broker<M> {
    pub fn new(uri: &str, manager: M) -> Self {
        let (tx, recv) = unbounded_channel();

        Self {
            conn: None,
            publisher: Publisher::new(tx),
            publisher_queue: PublisherQueue::new(recv),
            consumer: Consumer::new(),
            uri: uri.to_owned(),
            manager,
        }
    }

    /// Connect `Broker` to the AMQP endpoint, then declare Proxy's queue.
    pub async fn init(&mut self) -> Result<()> {
        let conn =
            Connection::connect(&self.uri, ConnectionProperties::default().with_tokio()).await?;

        // Create Publisher
        let channel = conn.create_channel().await?;
        M::declare_publisher(&channel).await?;
        self.publisher_queue.channel = Some(channel); // not sure whether that is required or not

        // Create Consumer
        let channel = conn.create_channel().await?;
        let amqp_consumer = M::declare_consumer(&channel).await?;
        self.consumer.consumer = Some(amqp_consumer);

        info!("Broker connected, channels created.");

        self.conn = Some(conn);

        Ok(())
    }

    /// Add and store listeners
    /// When a listener is added, it will bind the queue to the specified exchange name.
    pub fn add_listener(&mut self, listener: Arc<dyn BrokerListener>) {
        self.consumer
            .listeners
            .as_mut()
            .expect("No listeners found")
            .push(Listener::new(listener));
    }

    pub async fn publish<P>(&self, entity: &P, routing_key: &str) -> Result<()>
    where
        P: BrokerPublish + Serialize,
    {
        self.publisher.publish(entity, routing_key).await
    }

    pub async fn publish_raw(&self, exchange: &str, routing_key: &str, msg: &[u8]) -> Result<()> {
        self.publisher.publish_raw(exchange, routing_key, msg).await
    }

    /// Spawn the consumer and retry on connection interruption
    pub async fn spawn(&mut self) {
        loop {
            if let Err(err) = self.init().await {
                error!(%err, "amqp connection failed");
                sleep(RECONNECTION_DELAY).await;
                continue; // retry connection before hitting the spawn
            } else {
                info!("connected to amqp");
            }

            tokio::select! {
                err = self.consumer.consume() => {
                    if let Err(err) = &err {
                        error!(%err, "amqp consumer failed, trying to reconnect..");
                    }
                }
                err = self.publisher_queue.publish() => {
                    if let Err(err) = &err {
                        error!(%err, "amqp publisher failed, trying to reconnect..");
                    }
                }
            }
        }
    }

    pub fn publisher(&self) -> &Publisher {
        &self.publisher
    }
}

/// needs to be a MPSC queue, in order there's a disconnection, messages still could be added to the queue
/// but needs to wait to be connected to AMQP in order to process the queue and send the msg on the broker
/// maybe have a "PublisherQueue"
#[derive(Clone)]
pub struct Publisher {
    /// Here we choose Unbounded channel, because if a disconnection happens, a large number of
    /// messages can be produced while rabbitma has been disconnected.
    /// Is it a good thing to keep all messages in memory? Unsure yet, the binary memory can grow
    /// indefinitely if rabbitmq isn't available again. But at least it won't block the async runtime.
    tx: UnboundedSender<QueueMessage>,
}

impl Publisher {
    pub fn new(tx: UnboundedSender<QueueMessage>) -> Self {
        Self { tx }
    }

    /// Push item into memory queue before pushing it to amqp
    /// TODO: remove the async here!
    pub async fn publish<P>(&self, entity: &P, routing_key: &str) -> Result<()>
    where
        P: BrokerPublish + Serialize,
    {
        let serialized = bincode::serialize(entity)?;

        self.tx.send(QueueMessage::new(
            entity.exchange_name(),
            routing_key,
            serialized,
        ))?;

        Ok(())
    }

    /// Push without serializing, serializing has been made before calling this function
    pub async fn publish_raw(&self, exchange: &str, routing_key: &str, msg: &[u8]) -> Result<()> {
        self.tx
            .send(QueueMessage::new(exchange, routing_key, msg.to_owned()))?;

        Ok(())
    }
}

pub struct PublisherQueue {
    recv: UnboundedReceiver<QueueMessage>,
    channel: Option<Channel>,
}

impl PublisherQueue {
    fn new(recv: UnboundedReceiver<QueueMessage>) -> Self {
        Self {
            recv,
            channel: None,
        }
    }

    pub async fn send(&self, msg: QueueMessage) -> Result<PublisherConfirm> {
        // start prometheus duration timer
        let histogram_timer = STAT_PUBLISHER_DURATION
            .with_label_values(&[&msg.exchange, &msg.routing_key])
            .start_timer();

        let res = self
            .channel
            .as_ref()
            .unwrap()
            .basic_publish(
                &msg.exchange,
                &msg.routing_key,
                BasicPublishOptions::default(),
                &msg.content,
                BasicProperties::default(),
            )
            .await;

        // finish and compute the duration to prometheus
        histogram_timer.observe_duration();

        // let res = res.await?;
        res.map_err(|e| Error::Amqp(e))
    }

    /// Process the messages from the queue to AMQP
    pub async fn publish(&mut self) -> Result<()> {
        while let Some(msg) = self.recv.recv().await {
            self.send(msg).await?; // todo: spawn a task here?
        }

        Ok(())
    }
}

struct QueueMessage {
    exchange: String,
    routing_key: String,
    content: Vec<u8>,
}

impl QueueMessage {
    fn new(exchange: &str, routing_key: &str, content: Vec<u8>) -> QueueMessage {
        Self {
            exchange: exchange.to_owned(),
            routing_key: routing_key.to_owned(),
            content,
        }
    }
}

pub struct Listener {
    inner: Arc<dyn BrokerListener>, // Replace Box with Arc, because a Box can not be cloned.
    semaphore: Arc<Semaphore>,
}

impl Clone for Listener {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            semaphore: self.semaphore.clone(),
        }
    }
}

impl Listener {
    pub fn new(listener: Arc<dyn BrokerListener>) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(listener.max_concurrent_tasks())),
            inner: listener,
        }
    }

    fn listener(&self) -> &Arc<dyn BrokerListener> {
        &self.inner
    }

    fn max_concurrent_tasks(&self) -> usize {
        self.inner.max_concurrent_tasks()
    }
}

pub struct Consumer {
    consumer: Option<lapin::Consumer>,
    listeners: Option<Vec<Listener>>,
}

impl Consumer {
    pub fn new() -> Self {
        Self {
            consumer: None,
            listeners: Some(vec![]),
        }
    }

    /// Add and store listeners
    /// When a listener is added, it will bind the queue to the specified exchange name.
    pub fn add_listener(&mut self, listener: Arc<dyn BrokerListener>) {
        self.listeners
            .as_mut()
            .expect("No listeners found")
            .push(Listener::new(listener));
    }

    /// Consume messages by finding the appropriated listener.
    pub async fn consume(&mut self) -> Result<()> {
        info!(listeners = %self.listeners.unwrap().len(), "Broker consuming...");

        while let Some(message) = self.consumer.as_ref().unwrap().next().await {
            match message {
                Ok(delivery) => {
                    // info!("received message: {:?}", delivery);
                    let listener = self.listeners.as_ref().unwrap().iter().find(|listener| {
                        listener.listener().exchange_name() == delivery.exchange.as_str()
                    });

                    if let Some(listener) = listener {
                        // Listener found, try to consume the delivery
                        let listener = listener.clone();
                        let permits_available = listener.semaphore.available_permits() as i64; // i64 for prometheus
                        debug!(
                            "waiting for a permit ({}/{} available)",
                            permits_available,
                            permits_max = listener.max_concurrent_tasks()
                        );
                        STAT_CONCURRENT_TASK
                            .with_label_values(&[delivery.exchange.as_str(), "max"])
                            .set(listener.max_concurrent_tasks() as i64);

                        let permit = listener.semaphore.clone();
                        let permit = permit.acquire_owned().await?;
                        debug!("Got a permit, we can start to check");

                        STAT_CONCURRENT_TASK
                            .with_label_values(&[delivery.exchange.as_str(), "permits_used"])
                            .inc();

                        // consume the delivery asynchronously
                        task::spawn(consume_async(delivery, listener, permit));
                    } else {
                        // No listener found for that exchange
                        if let Err(err) = delivery.nack(BasicNackOptions::default()).await {
                            panic!("Can't find any registered listeners for `{}` exchange: {:?} + Failed to send nack: {}", &delivery.exchange, &delivery, err);
                        } else {
                            panic!(
                                "Can't find any registered listeners for `{}` exchange: {:?}",
                                &delivery.exchange, &delivery
                            );
                        }
                    }
                }
                Err(err) => {
                    error!(%err, "Error when receiving a delivery");
                    Err(err)? // force the binary to shutdown on any AMQP error received
                }
            }
        }
        Ok(())
    }
}

impl Clone for Consumer {
    fn clone(&self) -> Self {
        Self {
            consumer: self.consumer.clone(),
            listeners: self.listeners.clone(),
        }
    }
}

// async fn consume_async<L: BrokerListener + ?Sized>(
//     delivery: Delivery,
//     listener: Arc<L>,
//     channel: Channel,
// ) {
/// Consume the delivery async
async fn consume_async(delivery: Delivery, listener: Listener, permit: OwnedSemaphorePermit) {
    // start prometheus duration timer
    let histogram_timer = STAT_CONSUMER_DURATION
        .with_label_values(&[listener.inner.exchange_name()])
        .start_timer();

    // launch the consumer
    let res = listener.listener().consume(&delivery).await;
    drop(permit); // release the permit immediately

    STAT_CONCURRENT_TASK
        .with_label_values(&[delivery.exchange.as_str(), "permits_used"])
        .dec();

    // finish and compute the duration to prometheus
    histogram_timer.observe_duration();

    if let Err(requeue) = res {
        let mut options = BasicRejectOptions::default();
        options.requeue = requeue;

        if let Err(err_reject) = delivery.reject(options).await {
            error!(requeue, %err_reject, "Broker failed to send REJECT");
        } else {
            let exchange_name = listener.inner.exchange_name();
            let routing_key = delivery.routing_key;
            let redelivered = delivery.redelivered;

            warn!(requeue, %exchange_name, %routing_key, %redelivered, "Error during consumption of a delivery, `REJECT` sent");
        }
    } else {
        // Consumption went fine, we send ACK
        if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
            error!(
                %err, "Delivery consumed, but failed to send ACK back to the broker",
            );
        }
    }
}
