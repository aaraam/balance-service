use serde::{Deserialize, Serialize};
use crate::core::chains_meta::native_symbol_for;
use crate::http::error::ApiErrorBody;

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

    #[serde(default)]
    pub wallet_addresses: Vec<String>,

    #[serde(default)]
    pub solana_wallet_addresses: Vec<String>,

    #[serde(default)]
    pub tron_wallet_addresses: Vec<String>,

    #[serde(default)]
    pub doge_wallet_addresses: Vec<String>,
    #[serde(default)]
    pub btc_wallet_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceResponse {
    pub status: bool,

    #[serde(rename = "isComplete")]
    pub is_complete: bool,

    #[serde(rename = "hasChanged")]
    pub has_changed: bool,

    #[serde(rename = "requestKey")]
    pub request_key: String,

    pub result: serde_json::Value,

    /// Current progress stage.
    /// Present on all responses; helps Node poller
    /// trigger staged FCM notifications without polling extra endpoints.
    /// Values: "queued" | "evm_done" | "sol_done" | "complete"
    #[serde(rename = "progressStage", skip_serializing_if = "Option::is_none")]
    pub progress_stage: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorBody>,
}

const ZERO_18: &str = "0.000000000000000000";

pub fn trc20_contracts_from_request(req: &BalanceRequest) -> Vec<String> {
    req.contracts
        .iter()
        .find(|c| c.network_name == "trx")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default()
}

pub fn zero_result_from_request(req: &BalanceRequest) -> serde_json::Value {
    use serde_json::json;
    use serde_json::Map;

    let sol_mints = req.contracts
        .iter()
        .find(|c| c.network_name == "sol")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default();

    let trc20_contracts = trc20_contracts_from_request(req);
    let has_trx = !trc20_contracts.is_empty() || req.contracts.iter().any(|c| c.network_name == "trx");

    let mut data: Vec<serde_json::Value> = Vec::new();

    // EVM + TRON Bundled
    for w in &req.wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        // 1. Build EVM Networks
        for cg in &req.contracts {
            let net = cg.network_name.as_str();

            if net == "sol" || net == "trx" { continue; }

            let mut net_obj: Map<String, serde_json::Value> = Map::new();

            net_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));
            for addr in &cg.contract_addresses {
                net_obj.insert(addr.clone(), json!(ZERO_18));
            }
            balance_obj.insert(net.to_string(), json!(net_obj));
        }

        // 2. Inject Derived TRON Identity
        if has_trx {
            let mut trx_obj: Map<String, serde_json::Value> = Map::new();

            trx_obj.insert("trx".to_string(), json!(ZERO_18));
            for addr in &trc20_contracts {
                trx_obj.insert(addr.clone(), json!(ZERO_18));
            }
            balance_obj.insert("trx".to_string(), json!(trx_obj));
        }

        data.push(json!({ "walletAddress": w, "balance": balance_obj }));
    }

    // SOL
    for w in &req.solana_wallet_addresses {
        let mut sol_obj: Map<String, serde_json::Value> = Map::new();

        sol_obj.insert("sol".to_string(), json!(ZERO_18));
        for mint in &sol_mints {
            sol_obj.insert(mint.clone(), json!(ZERO_18));
        }
        data.push(json!({ "walletAddress": w, "balance": { "sol": sol_obj } }));
    }

    // Totals
    let mut totals: Map<String, serde_json::Value> = Map::new();

    for cg in &req.contracts {
        let net = cg.network_name.as_str();

        if net == "sol" || net == "trx" { continue; }
        
        let mut net_obj: Map<String, serde_json::Value> = Map::new();

        net_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));
        for addr in &cg.contract_addresses {
            net_obj.insert(addr.clone(), json!(ZERO_18));
        }
        totals.insert(net.to_string(), json!(net_obj));
    }

    if !req.solana_wallet_addresses.is_empty() {
        let mut sol_obj: Map<String, serde_json::Value> = Map::new();

        sol_obj.insert("sol".to_string(), json!(ZERO_18));
        for mint in &sol_mints {
            sol_obj.insert(mint.clone(), json!(ZERO_18));
        }
        totals.insert("sol".to_string(), json!(sol_obj));
    }

    if has_trx {
        let mut trx_obj: Map<String, serde_json::Value> = Map::new();

        trx_obj.insert("trx".to_string(), json!(ZERO_18));
        for addr in &trc20_contracts {
            trx_obj.insert(addr.clone(), json!(ZERO_18));
        }
        totals.insert("trx".to_string(), json!(trx_obj));
    }

    json!({
        "data": data,
        "total": { "balance": totals }
    })
}