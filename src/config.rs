use thiserror::Error;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_addr: String,
    pub mongodb_uri: String,
    pub mongodb_db_main: String,

    // Worker
    pub worker_enabled: bool,
    pub worker_poll_ms: u64,

    // NEW: slows down processing so you can SEE logs/state changes
    pub worker_slow_ms: u64,

    // Thirdweb
    pub thirdweb_client_id: String,

    // Solana (native + SPL via getTokenAccountsByOwner)
    pub solana_rpc_url: String,

    // NEW: outbound HTTP timeout for RPC calls (EVM + SOL)
    pub rpc_timeout_ms: u64,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing env var: {0}")] MissingEnv(&'static str),
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());

        let mongodb_uri = std::env
            ::var("MONGODB_URI")
            .map_err(|_| ConfigError::MissingEnv("MONGODB_URI"))?;

        let mongodb_db_main = std::env
            ::var("MONGODB_DB_MAIN")
            .map_err(|_| ConfigError::MissingEnv("MONGODB_DB_MAIN"))?;

        let thirdweb_client_id = std::env
            ::var("THIRD_WEB_CLIENT_ID")
            .map_err(|_| ConfigError::MissingEnv("THIRD_WEB_CLIENT_ID"))?;

        // optional env vars
        let worker_enabled = std::env
            ::var("WORKER_ENABLED")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        let worker_poll_ms = std::env
            ::var("WORKER_POLL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(500);

        let worker_slow_ms = std::env
            ::var("WORKER_SLOW_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let solana_rpc_url = std::env
            ::var("SOLANA_RPC_URL")
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

        // NEW
        let rpc_timeout_ms = std::env
            ::var("RPC_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(12_000);

        Ok(Self {
            bind_addr,
            mongodb_uri,
            mongodb_db_main,
            thirdweb_client_id,
            worker_enabled,
            worker_poll_ms,
            worker_slow_ms,
            solana_rpc_url,
            rpc_timeout_ms,
        })
    }
}
