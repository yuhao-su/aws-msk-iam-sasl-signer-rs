use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use chrono::Utc;
use std::{borrow::Cow, collections::HashMap, env};

use super::*;

const TEST_REGION: Region = Region::from_static("us-west-2");
const TEST_ENDPOINT: &'static str = "kafka.us-west-2.amazonaws.com";

#[test]
fn test_add_user_agent() {
    let signed_url_str = "https://kafka.us-west-2.amazonaws.com/?Action=kafka-cluster%3AConnect";
    let mut signed_url = Url::parse(signed_url_str).unwrap();
    let expected_url = format!("{signed_url_str}&{USER_AGENT_KEY}={LIB_NAME}");
    add_user_agent(&mut signed_url);
    assert!(signed_url.as_str().strip_prefix(&expected_url).is_some());
}

#[tokio::test]
async fn test_load_default_credentials() {
    let mock_creds = Credentials::from_keys("MOCK-ACCESS-KEY", "MOCK-SECRET-KEY", None);

    env::set_var("AWS_ACCESS_KEY_ID", mock_creds.access_key_id());
    env::set_var("AWS_SECRET_ACCESS_KEY", mock_creds.secret_access_key());
    let creds = load_default_credentials(TEST_REGION).await.unwrap();
    assert_eq!(mock_creds.access_key_id(), creds.access_key_id());
    assert_eq!(mock_creds.secret_access_key(), creds.secret_access_key());

    // Clean-up env variables.
    env::remove_var("AWS_ACCESS_KEY_ID");
    env::remove_var("AWS_SECRET_ACCESS_KEY");
}

#[tokio::test]
async fn test_construct_auth_token() {
    let mock_creds = Credentials::from_keys(
        "MOCK-ACCESS-KEY",
        "MOCK-SECRET-KEY",
        Some("MOCK-SESSION-TOKEN".to_string()),
    );

    let (token, expiry_ms) = construct_auth_token(TEST_REGION, mock_creds.clone())
        .await
        .unwrap();

    assert_eq!(verify_auth_token(token, expiry_ms, &mock_creds), 900);
}

#[tokio::test]
async fn test_generate_auth_token() {
    let mock_creds = Credentials::from_keys(
        "MOCK-ACCESS-KEY",
        "MOCK-SECRET-KEY",
        Some("MOCK-SESSION-TOKEN".to_string()),
    );

    env::set_var("AWS_ACCESS_KEY_ID", mock_creds.access_key_id());
    env::set_var("AWS_SECRET_ACCESS_KEY", mock_creds.secret_access_key());
    env::set_var("AWS_SESSION_TOKEN", mock_creds.session_token().unwrap());

    let (token, expiry_ms) = generate_auth_token(TEST_REGION).await.unwrap();

    assert_eq!(verify_auth_token(token, expiry_ms, &mock_creds), 900);

    // Clean-up env variables.
    env::remove_var("AWS_ACCESS_KEY_ID");
    env::remove_var("AWS_SECRET_ACCESS_KEY");
    env::remove_var("AWS_SESSION_TOKEN");
}

#[tokio::test]
async fn test_generate_auth_token_with_credentials_provider() {
    let mock_creds = Credentials::from_keys("TEST-ACCESS-KEY", "TEST-SECRET-KEY", None);

    let mock_credentials_provider = SharedCredentialsProvider::new(mock_creds.clone());

    let (token, expiry_ms) =
        generate_auth_token_from_credentials_provider(TEST_REGION, mock_credentials_provider)
            .await
            .unwrap();

    assert_eq!(verify_auth_token(token, expiry_ms, &mock_creds), 900);
}

#[test]
fn test_token_expires_in() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
    let credentials_expiring_at = |expiry: SystemTime| {
        Credentials::new(
            "MOCK-ACCESS-KEY",
            "MOCK-SECRET-KEY",
            Some("MOCK-SESSION-TOKEN".to_string()),
            Some(expiry),
            "test",
        )
    };

    let long_lived = Credentials::from_keys("MOCK-ACCESS-KEY", "MOCK-SECRET-KEY", None);
    assert_eq!(
        token_expires_in(&long_lived, now).unwrap(),
        Duration::from_secs(900)
    );

    let expire_in_an_hour = credentials_expiring_at(now + Duration::from_secs(3600));
    assert_eq!(
        token_expires_in(&expire_in_an_hour, now).unwrap(),
        Duration::from_secs(900)
    );

    let expire_in_600_7_secs = credentials_expiring_at(now + Duration::from_millis(600_700));
    assert_eq!(
        token_expires_in(&expire_in_600_7_secs, now).unwrap(),
        Duration::from_secs(600)
    );

    let expire_in_1_sec = credentials_expiring_at(now + Duration::from_secs(1));
    assert_eq!(
        token_expires_in(&expire_in_1_sec, now).unwrap(),
        Duration::from_secs(1)
    );

    let expire_in_999_ms = credentials_expiring_at(now + Duration::from_millis(999));
    assert!(token_expires_in(&expire_in_999_ms, now).is_err());

    let expired = credentials_expiring_at(now - Duration::from_secs(1));
    assert!(token_expires_in(&expired, now).is_err());
}

#[tokio::test]
async fn test_construct_auth_token_with_expiring_credentials() {
    let credentials_expiry = SystemTime::now() + Duration::from_secs(600);
    let mock_creds = Credentials::new(
        "MOCK-ACCESS-KEY",
        "MOCK-SECRET-KEY",
        Some("MOCK-SESSION-TOKEN".to_string()),
        Some(credentials_expiry),
        "test",
    );

    let (token, expiry_ms) = construct_auth_token(TEST_REGION, mock_creds.clone())
        .await
        .unwrap();

    assert!(verify_auth_token(token, expiry_ms, &mock_creds) < 600);
    let credentials_expiry_ms = credentials_expiry
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!(expiry_ms <= credentials_expiry_ms);
}

#[tokio::test]
async fn test_construct_auth_token_with_expired_credentials() {
    let mock_creds = Credentials::new(
        "MOCK-ACCESS-KEY",
        "MOCK-SECRET-KEY",
        Some("MOCK-SESSION-TOKEN".to_string()),
        Some(SystemTime::now() - Duration::from_secs(1)),
        "test",
    );

    assert!(construct_auth_token(TEST_REGION, mock_creds).await.is_err());
}

// Verifies the token and returns its `X-Amz-Expires` in seconds.
fn verify_auth_token(token: String, expiry_ms: i64, cred: &Credentials) -> i64 {
    assert_ne!(expiry_ms, 0);

    let decoded_signed_url_bytes = BASE64_URL_SAFE_NO_PAD.decode(token).unwrap();
    let decoded_signed_url = String::from_utf8(decoded_signed_url_bytes).unwrap();

    let parsed_url = Url::parse(&decoded_signed_url).unwrap();

    assert_eq!(parsed_url.scheme(), "https");
    assert_eq!(parsed_url.host_str(), Some(TEST_ENDPOINT));

    let params: HashMap<_, _> = parsed_url.query_pairs().collect();

    assert_eq!(
        params.get("Action"),
        Some(&Cow::Borrowed("kafka-cluster:Connect"))
    );
    assert_eq!(
        params.get("X-Amz-Algorithm"),
        Some(&Cow::Borrowed("AWS4-HMAC-SHA256"))
    );
    assert_eq!(
        params.get("X-Amz-Security-Token").cloned(),
        cred.session_token().map(|token| Cow::Borrowed(token))
    );
    assert_eq!(
        params.get("X-Amz-SignedHeaders"),
        Some(&Cow::Borrowed("host"))
    );

    let credential = params.get("X-Amz-Credential").unwrap();
    let split_credential: Vec<_> = credential.split('/').collect();
    assert_eq!(split_credential[0], cred.access_key_id());
    assert_eq!(split_credential[2], TEST_REGION.as_ref());
    assert_eq!(split_credential[3], "kafka-cluster");
    assert_eq!(split_credential[4], "aws4_request");
    let date_time =
        NaiveDateTime::parse_from_str(params.get("X-Amz-Date").unwrap(), "%Y%m%dT%H%M%SZ").unwrap();
    assert!(date_time.and_utc() < Utc::now());
    assert!(params
        .get(USER_AGENT_KEY)
        .unwrap()
        .strip_prefix("aws-msk-iam-sasl-signer-rs/")
        .is_some());

    let signing_time_ms = date_time.and_utc().timestamp_millis();
    let expiry_duration_seconds: i64 = params.get("X-Amz-Expires").unwrap().parse().unwrap();
    let expiry_duration_ms = expiry_duration_seconds * 1000;

    assert_eq!(expiry_ms, signing_time_ms + expiry_duration_ms);

    let current_ms = Utc::now().timestamp_millis();
    assert!(expiry_ms > current_ms);

    expiry_duration_seconds
}
