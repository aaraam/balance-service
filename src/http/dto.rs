use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractGroup {
    pub network_name: String,
    pub contract_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceRequest {
    /// If true, we should force enqueue refresh even if snapshot isn't stale (Phase 2+ behavior).
    /// Default = false so older clients don't break.
    #[serde(default)]
    pub hard_refresh: bool,

    /// EVM/ERC20 contracts grouped by network
    #[serde(default)]
    pub contracts: Vec<ContractGroup>,

    /// EVM wallet addresses
    #[serde(default)]
    pub wallet_addresses: Vec<String>,

    /// Solana wallets (non-EVM)
    #[serde(default)]
    pub solana_wallet_addresses: Vec<String>,

    /// Doge wallets (non-EVM)
    #[serde(default)]
    pub doge_wallet_addresses: Vec<String>,

    /// BTC wallets (non-EVM)
    #[serde(default)]
    pub btc_wallet_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceResponse {
    pub status: bool,
    pub result: serde_json::Value,
}

/// Legacy-compatible empty response shape.
/// We'll replace the internals with real balances in Phase 2+,
/// but this keeps the API stable.
pub fn empty_legacy_result() -> serde_json::Value {
    serde_json::json!({
        "data": [],
        "total": { "balance": {} }
    })
}
