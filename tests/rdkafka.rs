use std::time::Duration;

use aws_config::Region;
use aws_msk_iam_sasl_signer_rs::generate_auth_token;
use rdkafka::client::OAuthToken;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};

const REGION: &str = "us-east-2";
const KAFKA_BROKER: &str = "xxx";
const KAFKA_TOPIC: &str = "xxx";

struct IamConsumerContext {
    region: Region,
}

impl IamConsumerContext {
    fn new(region: Region) -> Self {
        Self { region }
    }
}

impl ConsumerContext for IamConsumerContext {}

impl ClientContext for IamConsumerContext {
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        let (token, expiration_time_ms) =
            tokio::runtime::Handle::current().block_on(tokio::time::timeout(
                Duration::from_secs(10),
                generate_auth_token(self.region.clone()),
            ))??;
        Ok(OAuthToken {
            token,
            principal_name: "".to_string(),
            lifetime_ms: expiration_time_ms,
        })
    }
}

#[tokio::test]
async fn test_rdkafka() {
    let mut config = ClientConfig::new();

    config.set("bootstrap.servers", KAFKA_BROKER);

    config.set("sasl.mechanism", "OAUTHBEARER");
    config.set("group.id", "test-aws-msk-iam-sasl-signer-rs");

    let region = Region::from_static(REGION);
    let context = IamConsumerContext::new(region);

    let consumer: StreamConsumer<IamConsumerContext> = config.create_with_context(context).unwrap();

    let partition_list = {
        let mut list = TopicPartitionList::new();
        let meta_data = consumer
            .fetch_metadata(Some(KAFKA_TOPIC), Duration::from_micros(10))
            .unwrap();
        let topic = meta_data.topics().first().unwrap();
        for partition in topic.partitions() {
            list.add_partition(KAFKA_TOPIC, partition.id());
        }
        list
    };

    consumer.assign(&partition_list).unwrap();

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
