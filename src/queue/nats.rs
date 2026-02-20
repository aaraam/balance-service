use crate::config::AppConfig;
use anyhow::Context;
use async_nats::jetstream;
use async_nats::jetstream::consumer;
use async_nats::jetstream::stream::{RetentionPolicy, StorageType};
use async_nats::{Client, HeaderMap};
use std::time::Duration;

/// Durable work-queue of `requestKey` jobs.
/// Mongo remains the source of truth for status/dedup; JetStream is delivery.
#[derive(Clone)]
pub struct NatsQueue {
    client: Client,
    js: jetstream::Context,
    stream: String,
    subject: String,
    durable: String,
    max_ack_pending: i64,
    ack_wait: Duration,
}

impl NatsQueue {
    pub async fn connect(cfg: &AppConfig) -> Result<Self, anyhow::Error> {
        let client = async_nats::connect(&cfg.nats_url)
            .await
            .with_context(|| format!("connect nats: {}", cfg.nats_url))?;
        let js = jetstream::new(client.clone());

        Ok(Self {
            client,
            js,
            stream: cfg.nats_stream.clone(),
            subject: cfg.nats_subject.clone(),
            durable: cfg.nats_durable.clone(),
            max_ack_pending: cfg.nats_max_ack_pending,
            ack_wait: Duration::from_secs(cfg.nats_ack_wait_secs),
        })
    }

    pub async fn ensure_stream(&self) -> Result<(), anyhow::Error> {
        if self.js.get_stream(&self.stream).await.is_ok() {
            return Ok(());
        }

        self.js
            .create_stream(jetstream::stream::Config {
                name: self.stream.clone(),
                subjects: vec![self.subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                storage: StorageType::File,
                // NOTE: max_ack_pending is NOT a stream config field in async-nats 0.46.
                // It belongs on the consumer config (see get_or_create_consumer).
                max_age: Duration::from_secs(60 * 60 * 24 * 7),
                ..Default::default()
            })
            .await
            .with_context(|| format!("create stream {}", self.stream))?;

        Ok(())
    }

    pub async fn publish(&self, request_key: &str) -> Result<(), anyhow::Error> {
        let mut headers = HeaderMap::new();
        // NATS expects a header value type like &str/String, not parse().
        headers.insert("Nats-Msg-Id", request_key.to_string());

        self.client
            .publish_with_headers(
                self.subject.clone(),
                headers,
                request_key.as_bytes().to_vec().into(),
            )
            .await
            .context("publish job")?;

        Ok(())
    }

    pub async fn get_or_create_consumer(&self) -> Result<consumer::PullConsumer, anyhow::Error> {
        let stream = self
            .js
            .get_stream(&self.stream)
            .await
            .with_context(|| format!("get stream {}", self.stream))?;

        if let Ok(c) = stream.get_consumer(&self.durable).await {
            return Ok(c);
        }

        let cfg = consumer::pull::Config {
            durable_name: Some(self.durable.clone()),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: self.ack_wait,
            max_ack_pending: self.max_ack_pending,
            ..Default::default()
        };

        let consumer = stream
            .create_consumer(cfg)
            .await
            .with_context(|| format!("create consumer {}", self.durable))?;

        Ok(consumer)
    }
}