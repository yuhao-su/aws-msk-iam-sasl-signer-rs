use std::thread;
use std::time::Duration;

use aws_config::Region;
use aws_msk_iam_sasl_signer::generate_auth_token;
use rdkafka::client::OAuthToken;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, ClientContext, Message};
use tokio::runtime::Handle;
use tokio::time::timeout;
use tracing_subscriber;

const REGION: &str = "us-east-2";
const KAFKA_BROKER: &str = "your-broker-address";
const KAFKA_TOPIC: &str = "your-topic-name";

struct IamConsumerContext {
    region: Region,
    rt: Handle,
}

impl IamConsumerContext {
    fn new(region: Region, rt: Handle) -> Self {
        Self { region, rt }
    }
}

impl ConsumerContext for IamConsumerContext {}

impl ClientContext for IamConsumerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        let region = self.region.clone();
        let handle = self.rt.clone();
        let (token, expiration_time_ms) = {
            let handle = thread::spawn(move || {
                handle.block_on(async {
                    timeout(Duration::from_secs(10), generate_auth_token(region.clone())).await
                })
            });
            handle.join().unwrap()??
        };
        Ok(OAuthToken {
            token,
            principal_name: "".to_string(),
            lifetime_ms: expiration_time_ms,
        })
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let mut config = ClientConfig::new();

    config.set("bootstrap.servers", KAFKA_BROKER);
    config.set("security.protocol", "SASL_SSL");
    config.set("sasl.mechanism", "OAUTHBEARER");
    config.set("group.id", "test-aws-msk-iam-sasl-signer-rs");

    let region = Region::from_static(REGION);
    let context = IamConsumerContext::new(region, Handle::current());

    let consumer: StreamConsumer<IamConsumerContext> = config.create_with_context(context).unwrap();

    // Uncomment the following code to get the partition list and assign the consumer to the partitions.
    // Please note that it's necessary to call `consumer.recv().now_or_never()` to refresh the OAUTHBEARER token
    // before calling `consumer.fetch_metadata()`.
    // See https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a988395722598f63396d7a1bedb22adaf
    // for details.
    //
    // assert!(consumer.recv().now_or_never().is_none());
    // let partition_list = {
    //     let mut list = TopicPartitionList::new();
    //     let meta_data = consumer
    //         .fetch_metadata(Some(KAFKA_TOPIC), Duration::from_secs(10))
    //         .unwrap();
    //     let topic = meta_data.topics().first().unwrap();
    //     for partition in topic.partitions() {
    //         list.add_partition(KAFKA_TOPIC, partition.id());
    //     }
    //     list
    // };
    // consumer.assign(&partition_list).unwrap();

    consumer.subscribe(&[KAFKA_TOPIC]).unwrap();

    loop {
        let msg = consumer.recv().await;
        match msg {
            Ok(msg) => {
                println!(
                    "Received message: {}",
                    String::from_utf8_lossy(msg.payload().unwrap_or(&[]))
                )
            }
            Err(e) => {
                println!("Received error: {}", e)
            }
        }
    }
}
