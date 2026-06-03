use bson::DateTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceSnapshotDoc {
    pub request_key: String,
    pub normalized_request: serde_json::Value,
    pub result: serde_json::Value,
    pub last_updated_at: DateTime,
    pub refresh_state: String,

    #[serde(default)]
    pub is_complete: bool,

    #[serde(default)]
    pub has_changed: bool,

    /// Tracks which phase the worker has completed.
    /// Values: "queued" | "evm_done" | "sol_done" | "complete"
    /// Absent on old snapshots — treated as None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_stage: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceRefreshJobDoc {
    pub request_key: String,
    pub status: String, // queued | running | done | failed
    pub attempts: i32,
    pub next_retry_at: Option<DateTime>,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoMarketPriceAssetDoc {
    pub id: String,
    pub symbol: String,
    pub current_price: String,
    pub price_change_percentage_24h: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_address: Option<String>,

    #[serde(default)]
    pub token_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoMarketPriceDoc {
    pub currency: String,
    pub source_url: String,
    pub source_count: i32,
    pub count: i32,
    pub assets: Vec<CryptoMarketPriceAssetDoc>,
    pub fetched_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoMarketTrackedTokenDoc {
    pub currency: String,
    pub tracking_key: String,
    pub coingecko_id: String,
    pub symbol: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_platform_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_address: Option<String>,

    #[serde(default)]
    pub token_addresses: Vec<String>,

    #[serde(default = "default_true")]
    pub enabled: bool,

    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenDecimalsCacheDoc {
    pub blockchain: String,
    pub contract_address: String,
    pub exists: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub decimals: Option<u32>,

    pub updated_at: DateTime,
}

fn default_true() -> bool {
    true
}
