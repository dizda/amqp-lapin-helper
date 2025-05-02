use clap::Parser;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tracing::debug;

use amqp_lapin_helper::{
    BasicConsumeOptions, BasicQosOptions, Broker, BrokerListener, BrokerManager, BrokerPublish,
    Delivery, FieldTable, Publisher, RateLimiter,
};
use lapin::{Channel, Consumer};

#[derive(Parser, Clone)]
struct WorkGenerator {
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
    #[arg(long, default_value = "127.0.0.1:3765")]
    serve_metrics: SocketAddr,
    #[arg(long, default_value = "amqp://guest:guest@127.0.0.1:5672/%2f")]
    uri: String,
    #[arg(long, default_value = "")]
    exchange: String,
    #[arg(long, default_value = "amqp-lapin-helper-demo-queue")]
    routing_key: String,
    #[arg(long, default_value_t = 10)]
    batch_size: usize,
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,
}

#[async_trait::async_trait]
impl BrokerManager for WorkGenerator {
    async fn declare_publisher(&self, _channel: &Channel) -> amqp_lapin_helper::Result<()> {
        Ok(())
    }

    async fn declare_consumer(
        &self,
        _channel: &Channel,
    ) -> amqp_lapin_helper::Result<Option<Consumer>> {
        Ok(None)
    }
}

#[derive(serde::Serialize)]
struct DummyMessage {
    #[serde(skip)]
    to_exchange: &'static str,
}

impl BrokerPublish for DummyMessage {
    fn exchange_name(&self) -> &'static str {
        self.to_exchange
    }
}

#[tokio::main]
async fn main() {
    let conf = WorkGenerator::parse();
    tracing_subscriber::fmt()
        .with_max_level(if conf.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .init();
    let _server = tokio::spawn(prometheus_hyper::Server::run(
        prometheus::default_registry(),
        conf.serve_metrics,
        std::future::pending(),
    ));
    let mut amqp = Broker::new(&conf.uri, conf.clone());
    let publ = amqp.publisher().clone();
    tokio::spawn(async move { amqp.spawn().await });
    let to_exchange = Box::leak(Box::new(conf.exchange.clone()));
    let mut interval = tokio::time::interval(Duration::from_millis(conf.interval_ms));
    loop {
        interval.tick().await;
        debug!(len = conf.batch_size, "publishing batch");
        for _ in 0..conf.batch_size {
            publ.publish(&DummyMessage { to_exchange }, &conf.routing_key)
                .unwrap();
        }
    }
}
