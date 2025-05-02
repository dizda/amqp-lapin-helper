use clap::Parser;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tracing::debug;

use amqp_lapin_helper::{
    BasicConsumeOptions, BasicQosOptions, Broker, BrokerListener, BrokerManager, Delivery,
    FieldTable, RateLimiter,
};
use lapin::{Channel, Consumer};

#[derive(Parser, Clone)]
struct WorkSimulator {
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
    #[arg(long, default_value = "127.0.0.1:2765")]
    serve_metrics: SocketAddr,
    #[arg(long, default_value = "amqp://guest:guest@127.0.0.1:5672/%2f")]
    uri: String,
    #[arg(long, default_value = "")]
    exchange: String,
    #[arg(long, default_value = "amqp-lapin-helper-demo-queue")]
    queue: String,
    #[arg(long, default_value_t = 0)]
    prefetch_limit: u16,
    #[arg(long, default_value_t = 1000)]
    concurrency_limit: usize,
    #[arg(long, default_value_t = 1000)]
    rate_limit_interval_ms: u64,
    #[arg(long, default_value_t = 50)]
    rate_limit_refill: usize,
    #[arg(long, default_value_t = 50)]
    rate_limit_max: usize,
    #[arg(long, default_value_t = 100)]
    avg_work_duration_ms: u64,
    #[arg(long, default_value_t = 10000)]
    outlier_work_duration_ms: u64,
    #[arg(long, default_value_t = 0.1)]
    outlier_chance: f32,
}

#[async_trait::async_trait]
impl BrokerManager for WorkSimulator {
    async fn declare_publisher(&self, _channel: &Channel) -> amqp_lapin_helper::Result<()> {
        Ok(())
    }

    async fn declare_consumer(
        &self,
        channel: &Channel,
    ) -> amqp_lapin_helper::Result<Option<Consumer>> {
        channel
            .basic_qos(self.prefetch_limit, BasicQosOptions::default())
            .await
            .unwrap();

        let consumer = channel
            .basic_consume(
                &self.queue,
                "demo-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(amqp_lapin_helper::Error::Amqp)?;

        Ok(Some(consumer))
    }
}

#[async_trait::async_trait]
impl BrokerListener for WorkSimulator {
    fn exchange_name(&self) -> &'static str {
        Box::leak(Box::new(self.exchange.clone()))
    }

    fn max_concurrent_tasks(&self) -> usize {
        self.concurrency_limit
    }

    fn task_rate_limit(&self) -> Option<RateLimiter> {
        Some(
            RateLimiter::builder()
                .interval(Duration::from_millis(self.rate_limit_interval_ms))
                .refill(self.rate_limit_refill)
                .max(self.rate_limit_max)
                .build(),
        )
    }

    async fn consume(&self, delivery: &Delivery) -> Result<(), bool> {
        let duration =
            Duration::from_millis(if rand::random_range(0.0..1.0) <= self.outlier_chance {
                self.outlier_work_duration_ms
            } else {
                self.avg_work_duration_ms
            });
        debug!(?duration, tag = delivery.delivery_tag, "starting work");
        tokio::time::sleep(duration).await;
        debug!(?duration, tag = delivery.delivery_tag, "finished work");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let conf = WorkSimulator::parse();
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
    amqp.add_listener(Arc::new(conf));
    amqp.spawn().await;
}
