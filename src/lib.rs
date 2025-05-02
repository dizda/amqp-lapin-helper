#![forbid(unsafe_code)]
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
use once_cell::sync::Lazy;
use prometheus::{
    opts, register_gauge_vec, register_histogram_vec, register_int_counter, register_int_gauge_vec,
    GaugeVec, HistogramVec, IntCounter, IntGaugeVec,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};
use tokio::task;
use tokio::time::{sleep, timeout};

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

static STAT_TIMED_OUT_TASK: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "amqp_consumer_timed_out_tasks",
        "Count of tasks that hit the emergency timeout",
    )
    .unwrap()
});

const EXPONENTIAL_SECONDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 40.0,
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

static STAT_PUBLISHER_MSG_QUEUE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "amqp_publisher_msg_queue",
        "The number of messages pending in the queue",
        &["exchange_name", "routing_key"],
    )
    .unwrap()
});

/// The reconnection delay when it detects an amqp disconnection
const RECONNECTION_DELAY: Duration = Duration::from_millis(1000);
const AMQP_READINESS_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("AMQP Client Readiness error: {0}")]
    ReadinessSignal(String),
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

    /// The emergency termination timeout for the `consume` method
    /// (Prefer having shorter fine-grained timeouts on inner operations, but this is the last resort against
    ///  tasks hanging forever and starving the queue. 5 minutes by default)
    fn task_timeout(&self) -> Duration {
        Duration::from_millis(1000 * 60 * 5)
    }

    /// Whether to requeue upon an emergency termination timeout
    fn requeue_on_timeout(&self) -> bool {
        false
    }

    /// The method that will be called in the struct impl on every messages received
    /// Err(false): reject.requeue = false
    /// Err(true): reject.requeue = true
    async fn consume(&self, delivery: &Delivery) -> std::result::Result<(), bool>;
}

#[async_trait]
pub trait BrokerManager {
    async fn declare_publisher(&self, channel: &Channel) -> Result<()>;

    /// Consumer's declaration is not required, some apps only need a publisher, vice & versa
    async fn declare_consumer(&self, channel: &Channel) -> Result<Option<lapin::Consumer>>;
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
    /// This will synchronize the parent app with amqp being ready to consume.
    ready_tx: Option<Sender<()>>,
    ready_rx: Option<Receiver<()>>,
}

impl<M: BrokerManager> Broker<M> {
    pub fn new(uri: &str, manager: M) -> Self {
        let (tx, recv) = unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();

        Self {
            conn: None,
            publisher: Publisher::new(tx),
            publisher_queue: PublisherQueue::new(recv),
            consumer: Consumer::new(),
            uri: uri.to_owned(),
            manager,
            ready_tx: Some(ready_tx),
            ready_rx: Some(ready_rx),
        }
    }

    /// Connect `Broker` to the AMQP endpoint, then declare Proxy's queue.
    pub async fn init(&mut self) -> Result<()> {
        let options = ConnectionProperties::default()
            // Use tokio executor and reactor.
            // At the moment the reactor is only available for unix.
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);

        let conn = Connection::connect(&self.uri, options).await?;

        // Create Publisher
        let channel = conn.create_channel().await?;
        self.manager.declare_publisher(&channel).await?;
        self.publisher_queue.channel = Some(channel); // not sure whether that is required or not

        // Create Consumer
        let channel = conn.create_channel().await?;
        let amqp_consumer = self.manager.declare_consumer(&channel).await?;
        self.consumer.consumer = amqp_consumer;

        info!("Broker connected, channels created.");

        self.conn = Some(conn);

        // send a signal to tell the software that AMQP is ready and listening for incoming messages
        if let Some(ready_tx) = self.ready_tx.take() {
            ready_tx.send(()).expect("Can't send the amqp ready signal");
            debug!("amqp has been initialized and is ready for the first time");
        }

        Ok(())
    }

    /// Add and store listeners
    /// When a listener is added, it will bind the queue to the specified exchange name.
    pub fn add_listener(&mut self, listener: Arc<dyn BrokerListener>) {
        self.consumer.listeners.push(Listener::new(listener));
    }

    pub fn publish<P>(&self, entity: &P, routing_key: &str) -> Result<()>
    where
        P: BrokerPublish + Serialize,
    {
        self.publisher.publish(entity, routing_key)
    }

    pub fn publish_raw(&self, exchange: &str, routing_key: &str, msg: &[u8]) -> Result<()> {
        self.publisher.publish_raw(exchange, routing_key, msg)
    }

    /// This will send a copy of the receiver to receive a signal that says the consumer & publisher are ready
    /// so we could start the software and making sure we've haven't missed any messages.
    pub fn ready_signal(&mut self) -> Ready {
        Ready {
            ready_rx: Some(
                self.ready_rx
                    .take()
                    .expect("amqp::ready_signal() has already been consumed"),
            ),
        }
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

/// This will send a copy of the receiver to receive a signal that says the consumer & publisher are ready
/// so we could start the software and making sure we've haven't missed any messages.
pub struct Ready {
    ready_rx: Option<Receiver<()>>,
}

impl Ready {
    /// This will send a copy of the receiver to receive a signal that says the consumer & publisher are ready
    /// so we could start the software and making sure we've haven't missed any messages.
    ///
    /// A timeout has been added to make sure k8s will restart the containers to avoid
    /// the container to wait indefinitely the readiness of amqp due to some potential configuration issue.
    pub async fn wait_to_be_ready(&mut self) -> Result<()> {
        let secs = AMQP_READINESS_TIMEOUT.as_secs();
        let fut = self
            .ready_rx
            .take()
            .expect("amqp has already been initialized");

        timeout(AMQP_READINESS_TIMEOUT, fut)
            .await
            .map_err(|_| Error::ReadinessSignal(format!("timed out after {secs}s")))?
            .map_err(|e| Error::ReadinessSignal(e.to_string()))
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
    fn new(tx: UnboundedSender<QueueMessage>) -> Self {
        Self { tx }
    }

    /// Push item into memory queue before pushing it to amqp
    pub fn publish<P>(&self, entity: &P, routing_key: &str) -> Result<()>
    where
        P: BrokerPublish + Serialize,
    {
        let serialized = bincode::serialize(entity)?;

        self.tx.send(QueueMessage::new(
            entity.exchange_name(),
            routing_key,
            serialized,
        ))?;

        STAT_PUBLISHER_MSG_QUEUE
            .with_label_values(&[entity.exchange_name(), routing_key])
            .inc();

        Ok(())
    }

    /// Push without serializing, serializing has been made before calling this function
    pub fn publish_raw(&self, exchange: &str, routing_key: &str, msg: &[u8]) -> Result<()> {
        self.tx
            .send(QueueMessage::new(exchange, routing_key, msg.to_owned()))?;

        STAT_PUBLISHER_MSG_QUEUE
            .with_label_values(&[exchange, routing_key])
            .inc();

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

    /// Process the messages from the queue to AMQP spawn a separate thread to publish
    /// the message and avoid network delays for new incoming messages
    ///
    /// TODO: make sure this doesn't cause any problem such as messaging order issue
    pub async fn publish(&mut self) -> Result<()> {
        // process the mpsc queue for new messages
        while let Some(msg) = self.recv.recv().await {
            // Clone the Channel to move it to an async thread
            let channel = self.channel.as_ref().unwrap().clone();

            // send the message on amqp separately
            tokio::spawn(async move {
                // start prometheus duration timer
                let histogram_timer = STAT_PUBLISHER_DURATION
                    .with_label_values(&[&msg.exchange, &msg.routing_key])
                    .start_timer();

                let res = channel
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

                // decrement the gauge about the number of pending msg in the queue
                STAT_PUBLISHER_MSG_QUEUE
                    .with_label_values(&[&msg.exchange, &msg.routing_key])
                    .dec();

                if let Err(err) = res {
                    error!(%err, "failed to publish an amqp message");
                }
            });
        }

        Ok(())
    }
}

pub struct QueueMessage {
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

#[derive(Default)]
pub struct Consumer {
    consumer: Option<lapin::Consumer>,
    listeners: Vec<Listener>,
}

impl Consumer {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add and store listeners
    /// When a listener is added, it will bind the queue to the specified exchange name.
    pub fn add_listener(&mut self, listener: Arc<dyn BrokerListener>) {
        self.listeners.push(Listener::new(listener));
    }

    /// Consume messages by finding the appropriated listener.
    pub async fn consume(&mut self) -> Result<()> {
        if self.listeners.is_empty() || self.consumer.is_none() {
            warn!("No listeners have been found, nothing will be consumed from amqp.");

            loop {
                // in case there's no listeners, in order to avoid breaking
                // the job spawn for the publisher, we make an infinite loop here.
                sleep(Duration::from_secs(60)).await;
            }
        } else {
            info!(listeners = %self.listeners.len(), "Broker consuming...");
        }

        while let Some(message) = self.consumer.as_mut().unwrap().next().await {
            match message {
                Ok(delivery) => {
                    // info!("received message: {:?}", delivery);
                    let listener = self.listeners.iter().find(|listener| {
                        listener.listener().exchange_name() == delivery.exchange.as_str()
                    });

                    if let Some(listener) = listener {
                        // Listener found, try to consume the delivery
                        let listener = listener.clone();
                        let permits_available = listener.semaphore.available_permits() as i64; // i64 for prometheus
                        debug!(
                            "waiting for a permit ({permits_available}/{permits_max} available)",
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
    let res = timeout(
        listener.listener().task_timeout(),
        listener.listener().consume(&delivery),
    )
    .await;
    drop(permit); // release the permit immediately

    STAT_CONCURRENT_TASK
        .with_label_values(&[delivery.exchange.as_str(), "permits_used"])
        .dec();

    // finish and compute the duration to prometheus
    histogram_timer.observe_duration();

    let res = match res {
        Ok(inner) => inner,
        Err(_) => {
            error!("Consume task timed out");
            STAT_TIMED_OUT_TASK.inc();
            Err(listener.listener().requeue_on_timeout())
        }
    };

    if let Err(requeue) = res {
        #[allow(clippy::needless_update)]
        let options = BasicRejectOptions {
            requeue,
            ..Default::default()
        };

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
