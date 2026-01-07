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

    /// Non-EVM wallets
    #[serde(default)]
    pub solana_wallet_addresses: Vec<String>,
    #[serde(default)]
    pub doge_wallet_addresses: Vec<String>,
    #[serde(default)]
    pub btc_wallet_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceResponseMeta {
    pub request_key: String,
    pub refresh_state: String, // idle | queued | running | failed
    pub last_updated_at_ms: i64,
    pub age_secs: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceResponse {
    pub status: bool,
    pub result: serde_json::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<BalanceResponseMeta>,
}

/// Keep legacy wrapper shape stable.
pub fn empty_legacy_result() -> serde_json::Value {
    serde_json::json!({
        "data": [],
        "total": { "balance": {} }
    })
}

/// Builds a “zeros” result that includes:
/// - EVM wallets (wallet_addresses)
/// - SOL wallets (solana_wallet_addresses) if "sol" contract-group exists OR sol wallets exist
pub fn zero_legacy_result_from_request(req: &BalanceRequest) -> serde_json::Value {
    use serde_json::{json, Map, Value};

    let mut data: Vec<Value> = Vec::new();

    // helper: find contract group by network name
    let cg_for = |name: &str| -> Option<&ContractGroup> {
        req.contracts.iter().find(|c| c.network_name == name)
    };

    // ---- EVM rows ----
    for wallet in &req.wallet_addresses {
        let mut balance_obj = Map::new();

        for cg in &req.contracts {
            let mut chain_obj = Map::new();

            // native always exists for EVM group
            let native_symbol = match cg.network_name.as_str() {
                "eth" => "eth",
                "bnb" => "bnb",
                "matic" => "matic",
                "op" => "op",
                other => other,
            };

            chain_obj.insert(native_symbol.to_string(), json!("0"));

            for token in &cg.contract_addresses {
                chain_obj.insert(token.clone(), json!("0"));
            }

            balance_obj.insert(cg.network_name.clone(), Value::Object(chain_obj));
        }

        data.push(json!({
            "walletAddress": wallet,
            "balance": balance_obj
        }));
    }

    // ---- SOL rows ----
    // We include SOL rows if user sent any sol wallets OR they asked for "sol" contract group
    let include_sol_rows = !req.solana_wallet_addresses.is_empty() || cg_for("sol").is_some();
    if include_sol_rows {
        let sol_cg = cg_for("sol");
        let sol_mints: Vec<String> = sol_cg
            .map(|c| c.contract_addresses.clone())
            .unwrap_or_default();

        for sol_wallet in &req.solana_wallet_addresses {
            let mut balance_obj = Map::new();
            let mut sol_chain_obj = Map::new();

            // native SOL symbol = "sol"
            sol_chain_obj.insert("sol".to_string(), json!("0"));

            // SPL token mints (treated like "token contracts")
            for mint in &sol_mints {
                sol_chain_obj.insert(mint.clone(), json!("0"));
            }

            balance_obj.insert("sol".to_string(), Value::Object(sol_chain_obj));

            data.push(json!({
                "walletAddress": sol_wallet,
                "balance": balance_obj
            }));
        }
    }

    // ---- totals ----
    let mut total_balance = Map::new();

    for cg in &req.contracts {
        let mut chain_total = Map::new();

        if cg.network_name == "sol" {
            chain_total.insert("sol".to_string(), json!("0"));
            for mint in &cg.contract_addresses {
                chain_total.insert(mint.clone(), json!("0"));
            }
            total_balance.insert("sol".to_string(), Value::Object(chain_total));
            continue;
        }

        let native_symbol = match cg.network_name.as_str() {
            "eth" => "eth",
            "bnb" => "bnb",
            "matic" => "matic",
            "op" => "op",
            other => other,
        };

        chain_total.insert(native_symbol.to_string(), json!("0"));

        for token in &cg.contract_addresses {
            chain_total.insert(token.clone(), json!("0"));
        }

        total_balance.insert(cg.network_name.clone(), Value::Object(chain_total));
    }

    json!({
        "data": data,
        "total": {
            "balance": total_balance
        }
    })
}
