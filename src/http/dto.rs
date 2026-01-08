// ==================================================
// balance-service\src\http\dto.rs
// ==================================================

use serde::{ Deserialize, Serialize };

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

    /// Non-EVM wallets (IGNORED by this service; kept for backward compatibility)
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

/// Build a fully-shaped "zero balances" response for EVM ONLY.
/// Note: Non-EVM (sol/btc/doge/trx) is intentionally ignored even if present in request.
pub fn zero_result_from_request(req: &BalanceRequest) -> serde_json::Value {
    use serde_json::json;
    use serde_json::Map;

    // ---- Build data rows for EVM wallets only ----
    let mut data: Vec<serde_json::Value> = Vec::new();

    for w in &req.wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        for cg in &req.contracts {
            let net = cg.network_name.as_str();

            let mut chain_obj: Map<String, serde_json::Value> = Map::new();
            chain_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));

            // ensure every requested token exists with zero
            for token_addr in &cg.contract_addresses {
                chain_obj.insert(token_addr.clone(), json!(ZERO_18));
            }

            balance_obj.insert(net.to_string(), json!(chain_obj));
        }

        data.push(
            json!({
            "walletAddress": w,
            "balance": balance_obj
        })
        );
    }

    // ---- Build totals for EVM contract groups only ----
    let mut totals: Map<String, serde_json::Value> = Map::new();

    for cg in &req.contracts {
        let net = cg.network_name.as_str();

        let mut total_chain_obj: Map<String, serde_json::Value> = Map::new();
        total_chain_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));
        for token_addr in &cg.contract_addresses {
            total_chain_obj.insert(token_addr.clone(), json!(ZERO_18));
        }

        totals.insert(net.to_string(), json!(total_chain_obj));
    }

    json!({
        "data": data,
        "total": { "balance": totals }
    })
}
