//! Sends one message to an MSK cluster with IAM access control.
//!
//! ```sh
//! AWS_REGION=us-east-1 cargo run --example producer -- <bootstrap-servers> <topic>
//! ```

mod common;

use std::error::Error;
use std::time::Duration;

use common::{client_config, MskIamContext};
use rdkafka::producer::{FutureProducer, FutureRecord};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let mut args = std::env::args().skip(1);
    let (Some(bootstrap_servers), Some(topic)) = (args.next(), args.next()) else {
        return Err("usage: producer <bootstrap-servers> <topic>".into());
    };

    let producer: FutureProducer<MskIamContext> =
        client_config(&bootstrap_servers).create_with_context(MskIamContext::from_env().await)?;

    let record = FutureRecord::to(&topic).key("key").payload("payload");
    let delivery = producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| e)?;
    println!(
        "Delivered to partition {}, offset {}",
        delivery.partition, delivery.offset
    );
    Ok(())
}
