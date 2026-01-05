use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractGroup {
    pub networkName: String,
    pub contractAddresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceRequest {
    pub contracts: Vec<ContractGroup>,
    pub walletAddresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceResponse {
    pub status: bool,
    pub result: serde_json::Value,
}

pub fn empty_legacy_result() -> serde_json::Value {
    serde_json::json!({
        "data": [],
        "total": { "balance": {} }
    })
}
