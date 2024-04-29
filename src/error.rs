use aws_credential_types::provider::error::CredentialsError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SignerError {
    #[error("failed to provide credentials: {0}")]
    ProvideCredentials(#[from] CredentialsError),
    #[error("failed constuct auth token: {0}")]
    ConstructAuthToken(String),
}
