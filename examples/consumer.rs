//! Prints the messages of a topic on an MSK cluster with IAM access control.
//!
//! ```sh
//! AWS_REGION=us-east-1 cargo run --example consumer -- <bootstrap-servers> <topic> [group-id]
//! ```

mod common;

use std::error::Error;

use common::{client_config, MskIamContext};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let mut args = std::env::args().skip(1);
    let (Some(bootstrap_servers), Some(topic)) = (args.next(), args.next()) else {
        return Err("usage: consumer <bootstrap-servers> <topic> [group-id]".into());
    };
    let group_id = args
        .next()
        .unwrap_or_else(|| "aws-msk-iam-sasl-signer-example".to_string());

    let consumer: StreamConsumer<MskIamContext> = client_config(&bootstrap_servers)
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .create_with_context(MskIamContext::from_env().await)?;

    // librdkafka fetches the first token when the client is polled (here, by `recv`). Broker calls
    // made before that, such as `fetch_metadata`, time out; poll once first if you need them, e.g.
    // with `consumer.recv().now_or_never()`.
    consumer.subscribe(&[&topic])?;
    loop {
        match consumer.recv().await {
            Ok(message) => println!(
                "Received: {}",
                String::from_utf8_lossy(message.payload().unwrap_or_default())
            ),
            Err(e) => eprintln!("Error: {e}"),
        }
    }
}
