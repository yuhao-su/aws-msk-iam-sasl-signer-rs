# AWS MSK IAM SASL Signer for Rust

[![CI status](https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs/actions/workflows/ci.yaml)
[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](./LICENSE)

`aws-msk-iam-sasl-signer-rs` is a AWS MSK IAM SASL signer for Rust. It is a port of the [aws-msk-iam-sasl-signer-go](https://github.com/aws/aws-msk-iam-sasl-signer-go).

## Usage
add the following to your `Cargo.toml`:
```toml
[dependencies]
aws-msk-iam-sasl-signer = "1.0.1"
```

## Example
check the [producer example](./examples/consumer.rs) and [consumer example](./examples/producer.rs) for more details.

## License
This library is licensed under the [Apache 2.0 License](./LICENSE).