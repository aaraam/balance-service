use crate::core::chains_meta::native_symbol_for;
use crate::db::models::CryptoMarketTrackedTokenDoc;
use crate::http::error::ApiErrorBody;
use crate::market::techbank::TechbankMarketPriceItem;
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
#[serde(rename_all = "camelCase")]
pub struct TokenDecimalsRequest {
    #[serde(alias = "networkName", alias = "network", alias = "chain")]
    pub blockchain: String,

    #[serde(alias = "contract_address", alias = "address")]
    pub contract_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenDecimalsResponse {
    pub status: bool,
    pub blockchain: String,
    pub contract_address: String,
    pub exists: bool,
    pub decimals: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorBody>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoMarketPriceQuery {
    pub currency: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoMarketPriceResponse {
    pub status: bool,
    pub currency: String,
    pub source_url: String,
    pub saved_to_db: bool,
    pub fetched_at: String,

    #[serde(rename = "value")]
    pub value: Vec<TechbankMarketPriceItem>,

    #[serde(rename = "Count")]
    pub count: usize,

    pub source_count: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracked_token_count: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub coingecko_updated_count: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub coingecko_error: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorBody>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddCryptoMarketTokenRequest {
    #[serde(alias = "id", alias = "coingecko_id")]
    pub coingecko_id: String,

    pub symbol: String,

    pub currency: Option<String>,

    #[serde(alias = "platformId", alias = "asset_platform_id")]
    pub asset_platform_id: Option<String>,

    #[serde(alias = "tokenAddress", alias = "contract_address")]
    pub contract_address: Option<String>,

    #[serde(default, alias = "token_addresses")]
    pub token_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoMarketTrackedTokenDto {
    pub currency: String,
    pub tracking_key: String,
    pub coingecko_id: String,
    pub symbol: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_platform_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_address: Option<String>,

    pub token_addresses: Vec<String>,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl From<&CryptoMarketTrackedTokenDoc> for CryptoMarketTrackedTokenDto {
    fn from(value: &CryptoMarketTrackedTokenDoc) -> Self {
        Self {
            currency: value.currency.clone(),
            tracking_key: value.tracking_key.clone(),
            coingecko_id: value.coingecko_id.clone(),
            symbol: value.symbol.clone(),
            asset_platform_id: value.asset_platform_id.clone(),
            contract_address: value.contract_address.clone(),
            token_addresses: value.token_addresses.clone(),
            enabled: value.enabled,
            created_at: value.created_at.to_string(),
            updated_at: value.updated_at.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddCryptoMarketTokenResponse {
    pub status: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<CryptoMarketTrackedTokenDto>,

    pub price_updated: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorBody>,
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

    let sol_mints = req
        .contracts
        .iter()
        .find(|c| c.network_name == "sol")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default();

    let trc20_contracts = trc20_contracts_from_request(req);
    let has_trx =
        !trc20_contracts.is_empty() || req.contracts.iter().any(|c| c.network_name == "trx");

    let mut data: Vec<serde_json::Value> = Vec::new();

    // EVM + TRON Bundled
    for w in &req.wallet_addresses {
        let mut balance_obj: Map<String, serde_json::Value> = Map::new();

        // 1. Build EVM Networks
        for cg in &req.contracts {
            let net = cg.network_name.as_str();

            if net == "sol" || net == "trx" {
                continue;
            }

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

        if net == "sol" || net == "trx" {
            continue;
        }

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
