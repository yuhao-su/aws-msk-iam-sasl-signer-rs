mod error;
#[cfg(test)]
mod test;

use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::{provider::ProvideCredentials, Credentials};
use aws_types::{region::Region, sdk_config::SharedCredentialsProvider};
use base64::Engine;
use chrono::NaiveDateTime;
use error::SignerError;
use std::time::{Duration, SystemTime};

use url::Url;

/// The key for the action type in the request.
const ACTION_TYPE: &str = "Action";
/// The specific action name for connecting to a Kafka cluster.
const ACTION_NAME: &str = "kafka-cluster:Connect";
/// The signing name for the Kafka cluster.
const SIGNING_NAME: &str = "kafka-cluster";
/// The key for the User-Agent parameter in the request.
const USER_AGENT_KEY: &str = "User-Agent";
/// The name of the library.
const LIB_NAME: &str = env!("CARGO_PKG_NAME");
/// Tepresents the default session name for assuming a role.
const DEFAULT_SESSION_NAME: &str = "MSKSASLDefaultSession";
/// The default expiration time in seconds.
const DEFAULT_EXPIRY_SECONDS: i64 = 900;
/// The template for the Kafka endpoint URL
macro_rules! ENDPOINT_URL_TEMPLATE {
    () => {
        "https://kafka.{}.amazonaws.com"
    };
}
/// Indicates whether credentials should be debugged
const AWS_DEBUG_CREDS: bool = false;
/// Libray package version
const VERSION: &str = env!("CARGO_PKG_VERSION");

type SignerResult<T> = Result<T, SignerError>;

/// [`generate_auth_token`] generates base64 encoded signed url as auth token from default credentials.
/// Loads the IAM credentials from default credentials provider chain.
pub async fn generate_auth_token(region: Region) -> SignerResult<(String, i64)> {
    let credentials = load_default_credentials(region.clone()).await?;

    construct_auth_token(region, credentials).await
}

/// [`generate_auth_token_from_profile`] generates base64 encoded signed url as auth token by loading IAM credentials from an AWS named profile.
pub async fn generate_auth_token_from_profile(
    region: Region,
    aws_profile: String,
) -> SignerResult<(String, i64)> {
    let credentials = load_credentials_from_profile(region.clone(), aws_profile).await?;

    construct_auth_token(region, credentials).await
}

/// [`generate_auth_token_from_role`] generates base64 encoded signed url as auth token by loading IAM credentials from an aws role Arn
pub async fn generate_auth_token_from_role(
    region: Region,
    role_arn: String,
    mut sts_session_name: String,
) -> SignerResult<(String, i64)> {
    if sts_session_name.is_empty() {
        sts_session_name = DEFAULT_SESSION_NAME.to_string();
    }
    let credentials =
        load_credentials_from_role_arn(region.clone(), role_arn, sts_session_name).await?;

    construct_auth_token(region, credentials).await
}

/// [`generate_auth_token_from_credentials_provider`] generates base64 encoded signed url as auth token by loading IAM credentials
/// from an aws credentials provider
pub async fn generate_auth_token_from_credentials_provider(
    region: Region,
    credentials_provider: SharedCredentialsProvider,
) -> SignerResult<(String, i64)> {
    let credentials = load_credentials_from_credentials_provider(credentials_provider).await?;

    construct_auth_token(region, credentials).await
}

// Loads credentials from the default credential chain.
async fn load_default_credentials(region: Region) -> SignerResult<Credentials> {
    let config = aws_config::from_env().region(region).load().await;

    load_credentials_from_credentials_provider(config.credentials_provider().unwrap()).await
}

// Loads credentials from a named aws profile.
async fn load_credentials_from_profile(
    region: Region,
    aws_profile: String,
) -> SignerResult<Credentials> {
    let config = aws_config::from_env()
        .region(region)
        .profile_name(aws_profile)
        .load()
        .await;

    load_credentials_from_credentials_provider(config.credentials_provider().unwrap()).await
}

/// Loads credentials from a named by assuming the passed role.
///
/// This implementation creates a new sts client for every call to get or refresh token. In order to avoid this, please
/// use your own credentials provider.
/// If you wish to use regional endpoint, please pass your own credentials provider.
async fn load_credentials_from_role_arn(
    region: Region,
    role_arn: String,
    sts_session_name: String,
) -> SignerResult<Credentials> {
    let config = aws_config::from_env().region(region.clone()).load().await;
    let role_provider = AssumeRoleProvider::builder(role_arn)
        .configure(&config)
        .region(region)
        .session_name(sts_session_name)
        .build()
        .await;
    let credentials = role_provider.provide_credentials().await?;
    Ok(credentials)
}

// Loads credentials from the credentials provider
async fn load_credentials_from_credentials_provider(
    credentials_provider: SharedCredentialsProvider,
) -> SignerResult<Credentials> {
    let credentials = credentials_provider.provide_credentials().await?;
    Ok(credentials)
}

// Constructs Auth Token.
async fn construct_auth_token(
    region: Region,
    credentials: Credentials,
) -> SignerResult<(String, i64)> {
    let endpoint_url = format!(ENDPOINT_URL_TEMPLATE! {}, region);

    #[cfg(debug_assertions)]
    if AWS_DEBUG_CREDS {
        log_caller_identity(region.clone(), credentials.clone()).await;
    }

    let mut url = build_url(&endpoint_url).map_err(|e| {
        SignerError::ConstructAuthToken(format!("failed to build request for signing: {e}"))
    })?;

    sign_url(&mut url, region, credentials).map_err(|e| {
        SignerError::ConstructAuthToken(format!("failed to sign request with aws sig v4: {e}"))
    })?;

    let expiration_time_ms = get_expiration_time_ms(&url).map_err(|e| {
        SignerError::ConstructAuthToken(format!(
            "failed to extract expiration from signed url: {e}"
        ))
    })?;

    add_user_agent(&mut url);

    Ok((base64_encode(url), expiration_time_ms))
}

// Build https url with `Action` parameters in order to sign.
fn build_url(endpoint_url: &str) -> Result<Url, String> {
    let mut url = Url::parse(endpoint_url).map_err(|e| format!("failed to parse url: {e}"))?;
    url.query_pairs_mut().append_pair(ACTION_TYPE, ACTION_NAME);
    Ok(url)
}

// Sign url with aws sig v4.
fn sign_url(url: &mut Url, region: Region, credentials: Credentials) -> Result<(), String> {
    use aws_sigv4::http_request::{
        sign, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
    };
    use aws_sigv4::sign::v4;

    let mut signing_settings = SigningSettings::default();
    signing_settings.signature_location = SignatureLocation::QueryParams;
    signing_settings.expires_in = Some(Duration::from_secs(DEFAULT_EXPIRY_SECONDS as u64));
    let identity = credentials.into();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region.as_ref())
        .name(SIGNING_NAME)
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .map_err(|e| format!("failed to build signing parameters: {e}"))?;
    let signable_request = SignableRequest::new(
        "GET",
        url.as_str(),
        std::iter::empty(),
        SignableBody::Bytes(&[]),
    )
    .expect("signable request");

    let sign_output = sign(signable_request, &signing_params.into())
        .map_err(|e| format!("failed to build sign request: {e}"))?;
    let (sign_instructions, _) = sign_output.into_parts();

    let mut url_queries = url.query_pairs_mut();
    for (name, value) in sign_instructions.params() {
        url_queries.append_pair(name, value);
    }
    Ok(())
}

// Parses the URL and gets the expiration time in millis associated with the signed url
fn get_expiration_time_ms(signed_url: &Url) -> Result<i64, String> {
    let (_name, value) = &signed_url
        .query_pairs()
        .find(|(name, _value)| name == "X-Amz-Date")
        .unwrap_or_else(|| ("".into(), "".into()));

    let date_time = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ")
        .map_err(|_e| format!("failed to parse 'X-Amz-Date' param {value} from signed url"))?;

    let signing_time_ms = date_time.and_utc().timestamp_millis();

    Ok(signing_time_ms + DEFAULT_EXPIRY_SECONDS * 1000)
}

// Base64 encode with raw url encoding.
fn base64_encode(signed_url: Url) -> String {
    use base64::prelude::BASE64_URL_SAFE_NO_PAD;
    BASE64_URL_SAFE_NO_PAD.encode(signed_url.as_str().as_bytes())
}

// Add user agent to the signed url
fn add_user_agent(signed_url: &mut Url) {
    let user_agent = format!("{LIB_NAME}/{VERSION}");
    signed_url
        .query_pairs_mut()
        .append_pair(USER_AGENT_KEY, &user_agent);
}

/// Log caller identity to debug which credentials are being picked up
#[cfg(debug_assertions)]
async fn log_caller_identity(region: Region, aws_credentials: Credentials) {
    let config = aws_config::from_env()
        .region(region)
        .credentials_provider(SharedCredentialsProvider::new(aws_credentials))
        .load()
        .await;

    let sts_client = aws_sdk_sts::Client::new(&config);

    match sts_client.get_caller_identity().send().await {
        Ok(id) => {
            dbg!(id);
        }
        Err(e) => {
            dbg!("failed to get caller identity", e);
        }
    };
}
