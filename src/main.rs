mod chains;
mod config;
mod core;
mod db;
mod evm;
mod http;
mod solana;
mod tron;
mod worker;
mod queue;

use axum::{
    routing::{get, post},
    Router,
};
use config::AppConfig;
use db::mongo::Mongo;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub struct AppState {
    pub cfg: AppConfig,
    pub mongo: Arc<Mongo>,
    pub queue: Arc<queue::nats::NatsQueue>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let cfg = AppConfig::from_env()?;
    let mongo = Mongo::connect(&cfg).await?;

    let queue = queue::nats::NatsQueue::connect(&cfg).await?;
    queue.ensure_stream().await?;

    let _ = db::snapshots::ensure_indexes(&mongo.db).await;
    let _ = db::refresh_jobs::ensure_indexes(&mongo.db).await;

    let state = AppState {
        cfg: cfg.clone(),
        mongo: Arc::new(mongo),
        queue: Arc::new(queue),
    };

    let worker_state = state.clone();
    tokio::spawn(async move {
        worker::runner::run_worker(worker_state).await;
    });

    let app = Router::new()
        .route("/health", get(http::handlers::health))
        .route(
            "/wallet/get-multi-wallet-balances",
            post(http::handlers::get_multi_wallet_balances),
        )
        .route(
            "/wallet/status/:request_key",
            get(http::handlers::get_job_status),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!("listening on {}", cfg.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}