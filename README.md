# AWS MSK IAM SASL Signer for Rust

[![crates.io](https://img.shields.io/crates/v/aws-msk-iam-sasl-signer.svg)](https://crates.io/crates/aws-msk-iam-sasl-signer)
[![docs.rs](https://img.shields.io/docsrs/aws-msk-iam-sasl-signer)](https://docs.rs/aws-msk-iam-sasl-signer)
[![CI status](https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs/actions/workflows/ci.yaml)
[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](./LICENSE)

`aws-msk-iam-sasl-signer` generates auth tokens for Amazon MSK clusters with IAM access control, for the SASL/OAUTHBEARER mechanism of a Kafka client. It is a port of [aws-msk-iam-sasl-signer-go](https://github.com/aws/aws-msk-iam-sasl-signer-go).

## Usage
Add the dependency to your `Cargo.toml`:
```toml
[dependencies]
aws-msk-iam-sasl-signer = "1"
```
See the [API documentation](https://docs.rs/aws-msk-iam-sasl-signer) for the functions and what they return.

## Example
The [producer](./examples/producer.rs) and [consumer](./examples/consumer.rs) examples connect [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) to an MSK cluster with IAM access control, taking the region and credentials from the default AWS configuration chain:
```sh
AWS_REGION=us-east-1 cargo run --example producer -- <bootstrap-servers> <topic>
AWS_REGION=us-east-1 cargo run --example consumer -- <bootstrap-servers> <topic> [group-id]
```

## License
This library is licensed under the [Apache 2.0 License](./LICENSE).