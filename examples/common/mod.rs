use std::error::Error;
use std::time::Duration;

use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use aws_types::region::Region;
use aws_types::sdk_config::SharedCredentialsProvider;
use rdkafka::client::OAuthToken;
use rdkafka::consumer::ConsumerContext;
use rdkafka::{ClientConfig, ClientContext};
use tokio::runtime::Handle;

/// Answers librdkafka's SASL/OAUTHBEARER token requests with MSK IAM auth tokens.
pub struct MskIamContext {
    region: Region,
    credentials_provider: SharedCredentialsProvider,
    runtime: Handle,
}

impl MskIamContext {
    /// Takes the region and credentials from the default AWS configuration chain
    /// (`AWS_REGION`, `AWS_PROFILE`, instance roles, ...).
    pub async fn from_env() -> Self {
        let config = aws_config::load_from_env().await;
        Self {
            region: config
                .region()
                .expect("no AWS region configured, set AWS_REGION")
                .clone(),
            credentials_provider: config
                .credentials_provider()
                .expect("no AWS credentials provider"),
            runtime: Handle::current(),
        }
    }
}

impl ClientContext for MskIamContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        // librdkafka calls this from the thread that polls the client: a Tokio worker for
        // `StreamConsumer::recv`, a plain thread for `FutureProducer`. `block_in_place` leaves the
        // worker, and the timer is created inside `block_on`, where a runtime is entered.
        let (token, lifetime_ms) = tokio::task::block_in_place(|| {
            self.runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    generate_auth_token_from_credentials_provider(
                        self.region.clone(),
                        self.credentials_provider.clone(),
                    ),
                )
                .await
            })
        })??;
        Ok(OAuthToken {
            token,
            principal_name: String::new(),
            lifetime_ms,
        })
    }
}

impl ConsumerContext for MskIamContext {}

/// Client config for an MSK cluster with IAM access control.
pub fn client_config(bootstrap_servers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_servers)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "OAUTHBEARER");
    config
}
