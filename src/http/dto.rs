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
    #[serde(default)]
    pub hard_refresh: bool,

    #[serde(default)]
    pub contracts: Vec<ContractGroup>,

    /// EVM wallets
    #[serde(default)]
    pub wallet_addresses: Vec<String>,

    /// Non-EVM wallets (currently stubbed to 0)
    #[serde(default)]
    pub solana_wallet_addresses: Vec<String>,
    #[serde(default)]
    pub doge_wallet_addresses: Vec<String>,
    #[serde(default)]
    pub btc_wallet_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceResponse {
    pub status: bool,
    pub result: serde_json::Value,
}

/// Keep legacy wrapper shape stable.
pub fn empty_legacy_result() -> serde_json::Value {
    serde_json::json!({
        "data": [],
        "total": { "balance": {} }
    })
}
