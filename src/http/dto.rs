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

/// NEW: Build a fully-shaped "zero balances" response that matches your standard.
pub fn zero_result_from_request(req: &BalanceRequest) -> serde_json::Value {
    use serde_json::json;
    use serde_json::Map;

    // ---- Build data rows for all wallets (EVM + non-EVM lists) ----
    let mut data: Vec<serde_json::Value> = Vec::new();

    // EVM wallets: include per-network objects for every requested contract group
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

    // Non-EVM wallets: minimal per-network object with a native symbol-like key
    // (until you actually integrate SOL/TRX/BTC/DOGE fetchers)
    for w in &req.solana_wallet_addresses {
        data.push(
            json!({
            "walletAddress": w,
            "balance": { "sol": { "sol": ZERO_18 } }
        })
        );
    }

    for w in &req.btc_wallet_addresses {
        data.push(
            json!({
            "walletAddress": w,
            "balance": { "btc": { "btc": ZERO_18 } }
        })
        );
    }

    for w in &req.doge_wallet_addresses {
        data.push(
            json!({
            "walletAddress": w,
            "balance": { "doge": { "doge": ZERO_18 } }
        })
        );
    }

    // ---- Build totals ----
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

    // also include non-evm totals if those wallet lists exist
    if !req.solana_wallet_addresses.is_empty() {
        totals.insert("sol".to_string(), json!({ "sol": ZERO_18 }));
    }
    if !req.btc_wallet_addresses.is_empty() {
        totals.insert("btc".to_string(), json!({ "btc": ZERO_18 }));
    }
    if !req.doge_wallet_addresses.is_empty() {
        totals.insert("doge".to_string(), json!({ "doge": ZERO_18 }));
    }

    json!({
        "data": data,
        "total": { "balance": totals }
    })
}
