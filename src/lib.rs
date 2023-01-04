#[forbid(unsafe_code)]
#[macro_use]
extern crate log;

use std::ffi::OsStr;
use std::path::Path;
pub use lapin::{
    message::Delivery, options::*, types::*, BasicProperties, Channel, Connection,
    ConnectionProperties, ExchangeKind, Queue,
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
use bincode::ErrorKind;
use futures_lite::StreamExt;
use lapin::publisher_confirm::Confirmation;
use serde::Serialize;
use std::sync::Arc;
use once_cell::sync::Lazy;
use prometheus::{Histogram, HistogramVec, IntGaugeVec, opts, register_histogram, register_histogram_vec, register_int_gauge_vec};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, SemaphorePermit};
use tokio::task;
use tokio::task::JoinHandle;
use tokio_amqp::*;

pub type Result<E> = std::result::Result<E, Error>;

/// It's not possible to get the binary name at compile time, so we get it here at runtime
static BINARY_NAME: Lazy<String> = Lazy::new(|| {
    format!("{}", std::env::current_exe().unwrap().iter().last().expect("can't find the name").to_string_lossy())
});

static STAT_CONCURRENT_TASK: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        opts!(
            format!("{}_{}", *BINARY_NAME, "consumer_concurrent_tasks"), // will generate `consumer_peer_check_consumer_concurrent_tasks`
            "Current/Max concurrent check",
        ),
        &["exchange_name", "kind"],
    ).unwrap()
});

const EXPONENTIAL_SECONDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

static STAT_CONSUMER_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        format!("{}_{}", *BINARY_NAME, "consumer_duration"),
        "The duration of the consumer",
        &["exchange_name"],
        EXPONENTIAL_SECONDS.to_vec(),
    ).unwrap()
});

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("acquire-semaphore: {0}")]
    AcquireSemaphore(#[from] AcquireError),

    #[error("AMQP: {0}")]
    Amqp(#[from] lapin::Error),

    #[error("Missing server ID")]
    MissingServerId,

    #[error("String UTF-8 error: {0}")]
    StringUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("SerdeJson: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Consumer: {0}")]
    ConsumerError(#[from] anyhow::Error),
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
    fn concurrent_ack(&self) -> usize {
        1
    }

    /// The method that will be called in the struct impl on every messages received
    async fn consume(&self, delivery: Delivery) -> Result<()>;
}

/// AMQP Client
pub struct Broker {
    conn: Option<Connection>,
    publisher: Publisher,
    consumer: Consumer,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            conn: None,
            publisher: Publisher::new(),
            consumer: Consumer::new(),
        }
    }

    /// Connect `Broker` to the AMQP endpoint, then declare Proxy's queue.
    pub async fn init(&mut self, uri: &str) -> Result<()> {
        let conn = Connection::connect(uri, ConnectionProperties::default().with_tokio()).await?;

        info!("Broker connected.");

        self.conn = Some(conn);

        Ok(())
    }

    /// Setup publisher
    pub async fn setup_publisher(&mut self) -> Result<&Publisher> {
        let channel = self.conn.as_ref().unwrap().create_channel().await?;
        self.publisher.channel = Some(channel);

        Ok(&self.publisher)
    }

    /// Init the consumer then return a mut instance in case we need to make more bindings
    pub async fn setup_consumer(&mut self) -> Result<&mut Consumer> {
        let channel = self.conn.as_ref().unwrap().create_channel().await?;
        self.consumer.channel = Some(channel);

        Ok(&mut self.consumer)
    }

    pub async fn publish<P>(&self, entity: &P, routing_key: &str)
    where
        P: BrokerPublish + Serialize,
    {
        self.publisher.publish(entity, routing_key).await;
    }

    pub async fn publish_raw(
        &self,
        exchange: &str,
        routing_key: &str,
        msg: Vec<u8>,
    ) -> Result<Confirmation> {
        self.publisher.publish_raw(exchange, routing_key, msg).await
    }
}

pub struct Publisher {
    channel: Option<Channel>,
}

impl Publisher {
    pub fn new() -> Self {
        Self { channel: None }
    }

    pub fn channel(&self) -> &Channel {
        self.channel.as_ref().expect("Publisher's channel is None")
    }

    /// Push item into amqp
    pub async fn publish<P>(&self, entity: &P, routing_key: &str)
    where
        P: BrokerPublish + Serialize,
    {
        let serialized = match bincode::serialize(entity) {
            Ok(msg) => msg,
            Err(e) => return error!("Unable to serialize msg: {}", e),
        };

        let res = self
            .channel()
            .basic_publish(
                entity.exchange_name(),
                routing_key,
                BasicPublishOptions::default(),
                serialized,
                BasicProperties::default(),
            )
            .await;

        if let Err(ref err) = res {
            // What shall we do, here?! as we can not break the publish method if we use `?`
            error!("Unable to publish msg: {}", err);
        }
        // no need to use the confirmation because ConfirmNotRequested (we don't need it for now)
    }

    /// Push without serializing
    pub async fn publish_raw(
        &self,
        exchange: &str,
        routing_key: &str,
        msg: Vec<u8>,
    ) -> Result<Confirmation> {
        let res = self
            .channel()
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                msg,
                BasicProperties::default(),
            )
            .await?;

        let res = res.await?;
        Ok(res)
    }
}

impl Clone for Publisher {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

pub struct Listener {
    inner: Arc<dyn BrokerListener>,  // Replace Box with Arc, because a Box can not be cloned.
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
            semaphore: Arc::new(Semaphore::new(listener.concurrent_ack())),
            inner: listener,
        }
    }

    fn listener(&self) -> &Arc<dyn BrokerListener> {
        &self.inner
    }

    fn max_concurrent(&self) -> usize {
        self.inner.concurrent_ack()
    }
}

pub struct Consumer {
    channel: Option<Channel>,
    consumer: Option<lapin::Consumer>,
    listeners: Option<Vec<Listener>>,
}

impl Consumer {
    pub fn new() -> Self {
        Self {
            channel: None,
            consumer: None,
            listeners: Some(vec![]),
        }
    }

    pub fn channel(&self) -> &Channel {
        self.channel.as_ref().expect("Consumer's channel is None")
    }

    pub fn set_consumer(&mut self, consumer: lapin::Consumer) {
        self.consumer = Some(consumer);
    }

    /// Add and store listeners
    /// When a listener is added, it will bind the queue to the specified exchange name.
    pub fn add_listener(&mut self, listener: Arc<dyn BrokerListener>) {
        self.listeners.as_mut().expect("No listeners found").push(Listener::new(listener));
    }

    pub fn spawn(&mut self) -> JoinHandle<Result<()>> {
        let consumer = self
            .consumer
            .as_ref()
            .expect("A consumer hasn't been set.")
            .clone();
        let listeners = self.listeners.take().expect("No listeners found");

        let handle = task::spawn(Consumer::consume(consumer, listeners));

        info!("Consumer has been launched in background.");

        handle
    }

    /// Consume messages by finding the appropriated listener.
    pub async fn consume(
        mut consumer: lapin::Consumer,
        listeners: Vec<Listener>,
    ) -> Result<()> {
        debug!("Broker consuming...");
        while let Some(message) = consumer.next().await {
            match message {
                Ok((channel, delivery)) => {
                    // info!("received message: {:?}", delivery);
                    let listener = listeners
                        .iter()
                        .find(|listener| listener.listener().exchange_name() == delivery.exchange.as_str());

                    if let Some(listener) = listener {
                        // Listener found, try to consume the delivery
                        let listener = listener.clone();
                        let permits_available = listener.semaphore.available_permits() as i64; // i64 for prometheus
                        debug!("waiting for a permit ({}/{} available)", permits_available, listener.max_concurrent());
                        STAT_CONCURRENT_TASK
                            .with_label_values(&[delivery.exchange.as_str(), "permits_available"])
                            .set(permits_available);
                        STAT_CONCURRENT_TASK
                            .with_label_values(&[delivery.exchange.as_str(), "max"])
                            .set(listener.max_concurrent() as i64);

                        let permit = listener.semaphore.clone();
                        let permit = permit.acquire_owned().await?;
                        debug!("Got a permit, we can start to check");

                        // consume the delivery asynchronously
                        task::spawn(consume_async(delivery, listener, channel, permit));
                    } else {
                        // No listener found for that exchange
                        if let Err(err) = channel
                            .basic_nack(delivery.delivery_tag, BasicNackOptions::default())
                            .await
                        {
                            panic!("Can't find any registered listeners for `{}` exchange: {:?} + Failed to send nack: {}", &delivery.exchange, &delivery, err);
                        } else {
                            panic!(
                                "Can't find any registered listeners for `{}` exchange: {:?}",
                                &delivery.exchange, &delivery
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Error when receiving a delivery: {}", e);
                    Err(e)? // force the binary to shutdown on any AMQP error received
                }
            }
        }
        Ok(())
    }
}

impl Clone for Consumer {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
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
async fn consume_async(
    delivery: Delivery,
    listener: Listener,
    channel: Channel,
    permit: OwnedSemaphorePermit,
) {
    let delivery_tag = delivery.delivery_tag;

    // start prometheus duration timer
    let histogram_timer = STAT_CONSUMER_DURATION.with_label_values(&[listener.inner.exchange_name()]).start_timer();

    // launch the consumer
    let res = listener.listener().consume(delivery).await;
    drop(permit); // release the permit immediately

    // finish and compute the duration to prometheus
    histogram_timer.observe_duration();


    // Then send the AMQP `ACK` or `NCK`
    if let Err(err) = res {
        // Consumption triggered an error, we send a NACK
        if let Err(err) = channel
            .basic_nack(delivery_tag, BasicNackOptions::default()) // can spawn this ASYNC
            .await
        {
            error!("Broker failed to send NACK: {:?}", err);
        } else {
            error!("Error during consumption of a delivery: {:?}, NACK sent", err);
        }
    } else {
        // Consumption went fine, we send ACK
        if let Err(err) = channel
            .basic_ack(delivery_tag, BasicAckOptions::default()) // can spawn this ASYNC
            .await
        {
            error!(
                "Broker's listener completed, but failed to send ACK back to the broker: {}",
                err
            );
        }
    }
}
