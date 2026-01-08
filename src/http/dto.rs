// ==================================================
// balance-service\src\http\dto.rs
// ==================================================

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

    /// Solana wallets (now supported for native SOL only)
    #[serde(default)]
    pub solana_wallet_addresses: Vec<String>,

    /// Non-supported (still ignored, kept for compatibility)
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

// Client asked for fixed 18-decimal zeros
const ZERO_18: &str = "0.000000000000000000";

fn native_symbol_for(network: &str) -> &str {
    match network {
        "eth" => "eth",
        "bnb" => "bnb",
        "matic" => "matic",
        "op" => "op",
        _ => network, // avax/ftm/cro/etc
    }
}

/// Build a fully-shaped "zero balances" response:
/// - EVM: native + token contracts per requested network
/// - SOL: native SOL only (no SPL tokens yet)
pub fn zero_result_from_request(req: &BalanceRequest) -> serde_json::Value {
    use serde_json::json;
    use serde_json::Map;

    let mut data: Vec<serde_json::Value> = Vec::new();

    // ---- EVM rows ----
    for w in &req.wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        for cg in &req.contracts {
            let net = cg.network_name.as_str();

            let mut chain_obj: Map<String, serde_json::Value> = Map::new();
            chain_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));

            for token_addr in &cg.contract_addresses {
                chain_obj.insert(token_addr.clone(), json!(ZERO_18));
            }

            balance_obj.insert(net.to_string(), json!(chain_obj));
        }

        data.push(json!({
            "walletAddress": w,
            "balance": balance_obj
        }));
    }

    // ---- SOL rows (native only) ----
    for w in &req.solana_wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        let mut sol_obj: Map<String, serde_json::Value> = Map::new();
        sol_obj.insert("sol".to_string(), json!(ZERO_18));
        balance_obj.insert("sol".to_string(), json!(sol_obj));

        data.push(json!({
            "walletAddress": w,
            "balance": balance_obj
        }));
    }

    // ---- Totals ----
    let mut totals: Map<String, serde_json::Value> = Map::new();

    // EVM totals per contract group
    for cg in &req.contracts {
        let net = cg.network_name.as_str();

        let mut total_chain_obj: Map<String, serde_json::Value> = Map::new();
        total_chain_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));
        for token_addr in &cg.contract_addresses {
            total_chain_obj.insert(token_addr.clone(), json!(ZERO_18));
        }

        totals.insert(net.to_string(), json!(total_chain_obj));
    }

    // SOL totals (native only)
    if !req.solana_wallet_addresses.is_empty() {
        let mut sol_total_obj: Map<String, serde_json::Value> = Map::new();
        sol_total_obj.insert("sol".to_string(), json!(ZERO_18));
        totals.insert("sol".to_string(), json!(sol_total_obj));
    }

    json!({
        "data": data,
        "total": { "balance": totals }
    })
}
