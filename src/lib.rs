#[forbid(unsafe_code)]
#[macro_use]
extern crate log;

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
use tokio::task;
use tokio::task::JoinHandle;
use tokio_amqp::*;

pub type Result<E> = std::result::Result<E, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
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
    fn routing_key(&self) -> &'static str;
}

pub enum ListenTo {
    Exchange(&'static str),
    Queue(&'static str),
}

/// Plug listeners to the broker.
#[async_trait]
pub trait BrokerListener: Send + Sync {
    /// Bind the queue & struct to this exchange name
    fn exchange_name(&self) -> &'static str;

    /// The method that will be called in the struct impl on every messages received
    async fn consume(&self, delivery: Delivery) -> Result<()>;
}

/// AMQP Client
pub struct Broker<L> {
    conn: Option<Connection>,
    publisher: Option<Publisher>,
    consumer: Option<Consumer<L>>,
}

impl<L: 'static + BrokerListener> Broker<L> {
    pub fn new() -> Self {
        Self {
            conn: None,
            publisher: None,
            consumer: None,
        }
    }

    /// Connect `Broker` to the AMQP endpoint, then declare Proxy's queue.
    pub async fn init(&mut self, uri: &str) -> Result<()> {
        let conn = Connection::connect(uri, ConnectionProperties::default().with_tokio()).await?;

        info!("Broker connected to {}.", uri);

        self.conn = Some(conn);

        Ok(())
    }

    /// Setup publisher
    pub async fn setup_publisher(&mut self) -> Result<&Publisher> {
        let channel = self.conn.as_ref().unwrap().create_channel().await?;
        let publisher = Publisher::new(channel);

        self.publisher = Some(publisher);

        Ok(self.publisher.as_ref().unwrap())
    }

    /// Init the consumer then return a mut instance in case we need to make more bindings
    pub async fn setup_consumer(&mut self) -> Result<&mut Consumer<L>> {
        let channel = self.conn.as_ref().unwrap().create_channel().await?;
        let consumer = Consumer::new(channel);

        self.consumer = Some(consumer);

        Ok(self.consumer.as_mut().unwrap())
    }

    pub async fn publish<P>(&self, entity: &P)
    where
        P: BrokerPublish + Serialize,
    {
        self.publisher.as_ref().unwrap().publish(entity).await;
    }
}

pub struct Publisher {
    channel: Channel,
}

impl Publisher {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Push item into amqp
    pub async fn publish<P>(&self, entity: &P)
    where
        P: BrokerPublish + Serialize,
    {
        let serialized = match bincode::serialize(entity) {
            Ok(msg) => msg,
            Err(e) => return error!("Unable to serialize msg: {}", e),
        };

        let res = self
            .channel
            .basic_publish(
                entity.exchange_name(),
                entity.routing_key(),
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
            .channel
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

pub struct Consumer<L> {
    channel: Channel,
    consumer: Option<lapin::Consumer>,
    listeners: Vec<Arc<L>>, // Replace Box with Arc, because a Box can not be cloned.
}

impl<L: 'static + BrokerListener> Consumer<L> {
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            consumer: None,
            listeners: vec![],
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn set_consumer(&mut self, consumer: lapin::Consumer) {
        self.consumer = Some(consumer);
    }

    /// Add and store listeners
    /// When a listener is added, it will bind the queue to the specified exchange name.
    pub fn add_listener(&mut self, listener: Arc<L>) {
        self.listeners.push(listener);
    }

    pub fn spawn(&self) -> JoinHandle<Result<()>> {
        // loop {
        let consumer = self
            .consumer
            .as_ref()
            .expect("A consumer hasn't been set.")
            .clone();
        let listeners = self.listeners.clone();

        let handle = task::spawn(Consumer::consume(consumer, listeners));

        info!("Consumer has been launched in background.");

        handle
    }

    /// Consume messages by finding the appropriated listener.
    pub async fn consume(mut consumer: lapin::Consumer, listeners: Vec<Arc<L>>) -> Result<()> {
        debug!("Broker consuming...");
        while let Some(message) = consumer.next().await {
            match message {
                Ok((channel, delivery)) => {
                    // info!("received message: {:?}", delivery);
                    let listener = listeners
                        .iter()
                        .find(|listener| listener.exchange_name() == delivery.exchange.as_str());

                    if let Some(listener) = listener {
                        // Listener found, try to consume the delivery
                        let listener = listener.clone();

                        // consume the delivery asynchronously
                        task::spawn(consume_async(delivery, listener, channel));
                    } else {
                        // No listener found for that exchange
                        if let Err(err) = channel
                            .basic_nack(delivery.delivery_tag, BasicNackOptions::default())
                            .await
                        {
                            error!("Can't find any registered listeners for `{}` exchange: {:?} + Failed to send nack: {}", &delivery.exchange, &delivery, err);
                        } else {
                            error!(
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

impl<L: BrokerListener> Clone for Consumer<L> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            consumer: self.consumer.clone(),
            listeners: self.listeners.clone(),
        }
    }
}

/// Consume the delivery async
async fn consume_async<L: BrokerListener>(delivery: Delivery, listener: Arc<L>, channel: Channel) {
    let delivery_tag = delivery.delivery_tag;

    if let Err(err) = listener.consume(delivery).await {
        // Consumption triggered an error, we send a NACK
        if let Err(err) = channel
            .basic_nack(delivery_tag, BasicNackOptions::default())
            .await
        {
            error!("Broker failed to send NACK: {}", err);
        } else {
            error!("Error during consumption of a delivery: {}, NACK sent", err);
        }
    } else {
        // Consumption went fine, we send ACK
        if let Err(err) = channel
            .basic_ack(delivery_tag, BasicAckOptions::default())
            .await
        {
            error!(
                "Broker's listener completed, but failed to send ACK back to the broker: {}",
                err
            );
        }
    }
}
