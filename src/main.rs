mod chains;
mod config;
mod core;
mod db;
mod evm;
mod http;
mod market;
mod queue;
mod solana;
mod tron;
mod worker;

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
    let _ = db::crypto_market_prices::ensure_indexes(&mongo.db).await;
    let _ = db::crypto_market_tracked_tokens::ensure_indexes(&mongo.db).await;
    let _ = db::token_decimals_cache::ensure_indexes(&mongo.db).await;

    let state = AppState {
        cfg: cfg.clone(),
        mongo: Arc::new(mongo),
        queue: Arc::new(queue),
    };

    let worker_state = state.clone();
    tokio::spawn(async move {
        worker::runner::run_worker(worker_state).await;
    });

    let market_price_state = state.clone();
    tokio::spawn(async move {
        market::service::run_market_price_refresher(market_price_state).await;
    });

    let app = Router::new()
        .route("/health", get(http::handlers::health))
        .route(
            "/token/get-decimals",
            post(http::handlers::get_token_decimals),
        )
        .route(
            "/crypto-market-price",
            get(http::handlers::refresh_crypto_market_prices),
        )
        .route(
            "/crypto-market-price/tokens",
            post(http::handlers::add_crypto_market_tracked_token),
        )
        .route(
            "/wallet/get-multi-wallet-balances",
            post(http::handlers::get_multi_wallet_balances),
        )
        .route(
            "/wallet/get-multi-wallet-balances-usd",
            post(http::handlers::get_multi_wallet_balances_usd),
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
