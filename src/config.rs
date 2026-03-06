use thiserror::Error;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_addr: String,
    pub mongodb_uri: String,
    pub mongodb_db_main: String,

    // Worker
    pub worker_enabled: bool,
    // legacy (mongo polling). kept for compatibility but unused in JetStream mode
    pub worker_poll_ms: u64,
    pub worker_concurrency: u32,

    // NEW: slows down processing so you can SEE logs/state changes
    pub worker_slow_ms: u64,

    // Queue (NATS JetStream)
    pub nats_url: String,
    pub nats_stream: String,
    pub nats_subject: String,
    pub nats_durable: String,
    pub nats_max_ack_pending: i64,
    pub nats_ack_wait_secs: u64,

    // Thirdweb
    pub thirdweb_client_id: String,

    // Solana (native + SPL via getTokenAccountsByOwner)
    pub solana_rpc_url: String,

    // NEW: outbound HTTP timeout for RPC calls (EVM + SOL)
    pub rpc_timeout_ms: u64,

    // ───────── TRON ─────────
    pub tron_fullnode_url: String,
    pub tron_solidity_url: String,
    pub tron_api_key: Option<String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing env var: {0}")]
    MissingEnv(&'static str),
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_addr = std::env::var("BIND_ADDR")
            .or_else(|_| std::env::var("BIND_ADDRESS"))
            .unwrap_or_else(|_| "[::]:3459".to_string());

        let mongodb_uri =
            std::env::var("MONGODB_URI").map_err(|_| ConfigError::MissingEnv("MONGODB_URI"))?;

        let mongodb_db_main = std::env::var("MONGODB_DB_MAIN")
            .map_err(|_| ConfigError::MissingEnv("MONGODB_DB_MAIN"))?;

        let thirdweb_client_id = std::env::var("THIRD_WEB_CLIENT_ID")
            .map_err(|_| ConfigError::MissingEnv("THIRD_WEB_CLIENT_ID"))?;

        // optional env vars
        let worker_enabled = std::env::var("WORKER_ENABLED")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        let worker_poll_ms = std::env::var("WORKER_POLL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(500);

        let worker_concurrency = std::env::var("WORKER_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(8);

        let worker_slow_ms = std::env::var("WORKER_SLOW_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let nats_url =
            std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
        let nats_stream =
            std::env::var("NATS_STREAM").unwrap_or_else(|_| "BALANCE_REFRESH".to_string());
        let nats_subject =
            std::env::var("NATS_SUBJECT").unwrap_or_else(|_| "balance.refresh".to_string());
        let nats_durable =
            std::env::var("NATS_DURABLE").unwrap_or_else(|_| "balance-worker".to_string());

        let nats_max_ack_pending = std::env::var("NATS_MAX_ACK_PENDING")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(10_000);

        let nats_ack_wait_secs = std::env::var("NATS_ACK_WAIT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120);

        let solana_rpc_url = std::env::var("SOLANA_RPC_URL")
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

        let rpc_timeout_ms = std::env::var("RPC_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(12_000);

        let tron_fullnode_url = std::env::var("TRON_FULLNODE_URL").unwrap_or_default();
        let tron_solidity_url = std::env::var("TRON_SOLIDITY_URL").unwrap_or_default();

        // Accept sane env name first, but keep backward compatibility
        let tron_api_key = std::env::var("TRON_API_KEY")
            .ok()
            .or_else(|| std::env::var("TRON_TEMP_KEY").ok());

        Ok(Self {
            bind_addr,
            mongodb_uri,
            mongodb_db_main,
            thirdweb_client_id,
            worker_enabled,
            worker_poll_ms,
            worker_concurrency,
            worker_slow_ms,
            nats_url,
            nats_stream,
            nats_subject,
            nats_durable,
            nats_max_ack_pending,
            nats_ack_wait_secs,
            solana_rpc_url,
            rpc_timeout_ms,
            tron_fullnode_url,
            tron_solidity_url,
            tron_api_key,
        })
    }
}