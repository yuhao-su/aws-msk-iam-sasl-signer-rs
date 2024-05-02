use std::thread;
use std::time::Duration;

use aws_config::Region;
use aws_msk_iam_sasl_signer_rs::generate_auth_token;
use rdkafka::client::OAuthToken;
use rdkafka::producer::{FutureProducer, FutureRecord, ProducerContext};
use rdkafka::{ClientConfig, ClientContext};
use tokio::runtime::Handle;
use tokio::time::timeout;
use tracing_subscriber;

const REGION: &str = "us-east-2";
const KAFKA_BROKER: &str = "your-broker-address";
const KAFKA_TOPIC: &str = "your-topic-name";

struct IamProducerContext {
    region: Region,
    rt: Handle,
}

impl IamProducerContext {
    fn new(region: Region, rt: Handle) -> Self {
        Self { region, rt }
    }
}

impl ProducerContext for IamProducerContext {
    type DeliveryOpaque = ();
    fn delivery(
        &self,
        _delivery_result: &rdkafka::message::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
    }
}

impl ClientContext for IamProducerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        let region = self.region.clone();
        let rt = self.rt.clone();
        let (token, expiration_time_ms) = {
            let handle = thread::spawn(move || {
                rt.block_on(async {
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

    let region = Region::from_static(REGION);
    let context = IamProducerContext::new(region, Handle::current());

    let producer: FutureProducer<IamProducerContext> = config.create_with_context(context).unwrap();

    let test_record = FutureRecord {
        topic: KAFKA_TOPIC,
        key: Some("test_key"),
        payload: Some("test_payload"),
        partition: None,
        timestamp: None,
        headers: None,
    };
    let (partition, offset) = producer
        .send(test_record, Duration::from_secs(5))
        .await
        .unwrap();
    println!("Message send to partition {partition}, offset {offset}");
}
