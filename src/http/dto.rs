// ==================================================
// FILE: D:\Learn\rust\balance-service\src\http\dto.rs
// ==================================================

use serde::{Deserialize, Serialize};

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

    /// EVM wallets
    #[serde(default)]
    pub wallet_addresses: Vec<String>,

    /// Solana wallets
    #[serde(default)]
    pub solana_wallet_addresses: Vec<String>,

    /// TRON wallets (base58check, starts with 'T')
    #[serde(default)]
    pub tron_wallet_addresses: Vec<String>,

    /// Non-supported (still ignored, kept for compatibility)
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

    // ✅ FIXED: Added this missing field
    #[serde(rename = "requestKey")]
    pub request_key: String,

    pub result: serde_json::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorBody>,
}

// fixed 18-decimal zeros
const ZERO_18: &str = "0.000000000000000000";

fn native_symbol_for(network: &str) -> &str {
    match network {
        "eth" => "eth",
        "bnb" => "bnb",
        "matic" => "matic",
        "op" => "op",
        "sol" => "sol",
        "trx" => "trx",
        _ => network, // avax/ftm/cro/etc
    }
}

fn sol_mints_from_request(req: &BalanceRequest) -> Vec<String> {
    req.contracts
        .iter()
        .find(|c| c.network_name == "sol")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default()
}

fn tron_contracts_from_request(req: &BalanceRequest) -> Vec<String> {
    req.contracts
        .iter()
        .find(|c| c.network_name == "trx")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default()
}

pub fn zero_result_from_request(req: &BalanceRequest) -> serde_json::Value {
    use serde_json::json;
    use serde_json::Map;

    let sol_mints = sol_mints_from_request(req);
    let tron_contracts = tron_contracts_from_request(req);

    // If TRX contracts are requested but tronWalletAddresses is empty,
    // we treat TRX as "derived from EVM wallets" and include trx under EVM rows.
    let tron_requested = !tron_contracts.is_empty();
    let tron_derived_from_evm = tron_requested && req.tron_wallet_addresses.is_empty();

    let mut data: Vec<serde_json::Value> = Vec::new();

    // ---- EVM rows ----
    for w in &req.wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        for cg in &req.contracts {
            let net = cg.network_name.as_str();
            if net == "sol" || net == "trx" {
                continue;
            }

            let mut chain_obj: Map<String, serde_json::Value> = Map::new();
            chain_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));

            for token_addr in &cg.contract_addresses {
                chain_obj.insert(token_addr.clone(), json!(ZERO_18));
            }

            balance_obj.insert(net.to_string(), json!(chain_obj));
        }

        // ✅ If TRX is requested and we are deriving TRON wallets from EVM wallets,
        // include balance.trx for this same wallet row.
        if tron_derived_from_evm {
            let mut trx_obj: Map<String, serde_json::Value> = Map::new();
            trx_obj.insert("trx".to_string(), json!(ZERO_18));
            for c in &tron_contracts {
                trx_obj.insert(c.clone(), json!(ZERO_18));
            }
            balance_obj.insert("trx".to_string(), json!(trx_obj));
        }

        data.push(json!({
            "walletAddress": w,
            "balance": balance_obj
        }));
    }

    // ---- SOL rows (native + optional SPL mints) ----
    for w in &req.solana_wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        let mut sol_obj: Map<String, serde_json::Value> = Map::new();
        sol_obj.insert("sol".to_string(), json!(ZERO_18));

        for mint in &sol_mints {
            sol_obj.insert(mint.clone(), json!(ZERO_18));
        }

        balance_obj.insert("sol".to_string(), json!(sol_obj));

        data.push(json!({
            "walletAddress": w,
            "balance": balance_obj
        }));
    }

    // ---- TRX rows (native + optional TRC20 contracts) ----
    // Keep legacy behavior if tronWalletAddresses is explicitly provided.
    for w in &req.tron_wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        let mut trx_obj: Map<String, serde_json::Value> = Map::new();
        trx_obj.insert("trx".to_string(), json!(ZERO_18));

        for c in &tron_contracts {
            trx_obj.insert(c.clone(), json!(ZERO_18));
        }

        balance_obj.insert("trx".to_string(), json!(trx_obj));

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
        if net == "sol" || net == "trx" {
            continue;
        }

        let mut total_chain_obj: Map<String, serde_json::Value> = Map::new();
        total_chain_obj.insert(native_symbol_for(net).to_string(), json!(ZERO_18));
        for token_addr in &cg.contract_addresses {
            total_chain_obj.insert(token_addr.clone(), json!(ZERO_18));
        }

        totals.insert(net.to_string(), json!(total_chain_obj));
    }

    // SOL totals (native + optional SPL mints)
    if !req.solana_wallet_addresses.is_empty() {
        let mut sol_total_obj: Map<String, serde_json::Value> = Map::new();
        sol_total_obj.insert("sol".to_string(), json!(ZERO_18));
        for mint in &sol_mints {
            sol_total_obj.insert(mint.clone(), json!(ZERO_18));
        }
        totals.insert("sol".to_string(), json!(sol_total_obj));
    }

    // ✅ TRX totals if:
    // - tronWalletAddresses provided OR
    // - tron requested and derived-from-evm mode (walletAddresses used)
    if tron_requested && (!req.tron_wallet_addresses.is_empty() || !req.wallet_addresses.is_empty())
    {
        let mut trx_total_obj: Map<String, serde_json::Value> = Map::new();
        trx_total_obj.insert("trx".to_string(), json!(ZERO_18));
        for c in &tron_contracts {
            trx_total_obj.insert(c.clone(), json!(ZERO_18));
        }
        totals.insert("trx".to_string(), json!(trx_total_obj));
    }

    json!({
        "data": data,
        "total": { "balance": totals }
    })
}