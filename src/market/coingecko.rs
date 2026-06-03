use crate::db::models::{CryptoMarketPriceAssetDoc, CryptoMarketTrackedTokenDoc};
use reqwest::Url;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

const MAX_SIMPLE_PRICE_BATCH: usize = 100;

#[derive(Debug, Error)]
pub enum CoinGeckoPriceError {
    #[error("invalid CoinGecko API base URL: {0}")]
    InvalidBaseUrl(String),

    #[error("failed to build CoinGecko HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),

    #[error("CoinGecko request failed: {0}")]
    Request(#[source] reqwest::Error),

    #[error("CoinGecko response was not valid JSON: {0}")]
    Decode(#[source] reqwest::Error),
}

#[derive(Debug, Clone)]
struct PriceValue {
    price: String,
    price_change_percentage_24h: String,
}

pub async fn fetch_tracked_token_prices(
    base_url: &str,
    api_key: Option<&str>,
    currency: &str,
    tracked_tokens: &[CryptoMarketTrackedTokenDoc],
    timeout_ms: u64,
) -> Result<Vec<CryptoMarketPriceAssetDoc>, CoinGeckoPriceError> {
    if tracked_tokens.is_empty() {
        return Ok(Vec::new());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build()
        .map_err(CoinGeckoPriceError::ClientBuild)?;

    let native_prices =
        fetch_native_prices(&client, base_url, api_key, currency, tracked_tokens).await?;
    let contract_prices =
        fetch_contract_prices(&client, base_url, api_key, currency, tracked_tokens).await?;

    let mut out = Vec::new();

    for token in tracked_tokens {
        let price = if let Some(contract_address) = token.contract_address.as_deref() {
            contract_prices.get(&contract_price_key(
                token.asset_platform_id.as_deref().unwrap_or_default(),
                contract_address,
            ))
        } else {
            native_prices.get(&token.coingecko_id.to_ascii_lowercase())
        };

        let Some(price) = price else {
            continue;
        };

        out.push(CryptoMarketPriceAssetDoc {
            id: token.coingecko_id.clone(),
            symbol: token.symbol.clone(),
            current_price: price.price.clone(),
            price_change_percentage_24h: price.price_change_percentage_24h.clone(),
            token_address: token.contract_address.clone(),
            token_addresses: token.token_addresses.clone(),
        });
    }

    Ok(out)
}

async fn fetch_native_prices(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    currency: &str,
    tracked_tokens: &[CryptoMarketTrackedTokenDoc],
) -> Result<HashMap<String, PriceValue>, CoinGeckoPriceError> {
    let ids = tracked_tokens
        .iter()
        .filter(|token| token.contract_address.is_none())
        .map(|token| token.coingecko_id.trim())
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();

    let mut out = HashMap::new();

    for chunk in ids.chunks(MAX_SIMPLE_PRICE_BATCH) {
        let mut url = endpoint_url(base_url, "simple/price")?;
        url.query_pairs_mut()
            .append_pair("ids", &chunk.join(","))
            .append_pair("vs_currencies", currency)
            .append_pair("include_24hr_change", "true");

        let raw = send_json(client, base_url, url, api_key).await?;
        let Some(obj) = raw.as_object() else {
            continue;
        };

        let by_id = obj
            .iter()
            .map(|(id, value)| (id.to_ascii_lowercase(), value))
            .collect::<HashMap<_, _>>();

        for id in chunk {
            if let Some(value) = by_id
                .get(&id.to_ascii_lowercase())
                .and_then(|value| value.as_object())
                .and_then(|obj| price_value_from_object(obj, currency))
            {
                out.insert(id.to_ascii_lowercase(), value);
            }
        }
    }

    Ok(out)
}

async fn fetch_contract_prices(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    currency: &str,
    tracked_tokens: &[CryptoMarketTrackedTokenDoc],
) -> Result<HashMap<String, PriceValue>, CoinGeckoPriceError> {
    let mut by_platform: HashMap<String, Vec<&CryptoMarketTrackedTokenDoc>> = HashMap::new();

    for token in tracked_tokens {
        let Some(platform) = token.asset_platform_id.as_deref() else {
            continue;
        };
        if token.contract_address.is_none() {
            continue;
        }

        by_platform
            .entry(platform.trim().to_string())
            .or_default()
            .push(token);
    }

    let mut out = HashMap::new();

    for (platform, tokens) in by_platform {
        let contracts = tokens
            .iter()
            .filter_map(|token| token.contract_address.as_deref())
            .map(str::trim)
            .filter(|contract| !contract.is_empty())
            .collect::<Vec<_>>();

        for chunk in contracts.chunks(MAX_SIMPLE_PRICE_BATCH) {
            let mut url = endpoint_url(base_url, &format!("simple/token_price/{platform}"))?;
            url.query_pairs_mut()
                .append_pair("contract_addresses", &chunk.join(","))
                .append_pair("vs_currencies", currency)
                .append_pair("include_24hr_change", "true");

            let raw = send_json(client, base_url, url, api_key).await?;
            let Some(obj) = raw.as_object() else {
                continue;
            };

            for (contract, value) in obj {
                let Some(price) = value
                    .as_object()
                    .and_then(|obj| price_value_from_object(obj, currency))
                else {
                    continue;
                };

                out.insert(contract_price_key(&platform, contract), price);
            }
        }
    }

    Ok(out)
}

async fn send_json(
    client: &reqwest::Client,
    base_url: &str,
    url: Url,
    api_key: Option<&str>,
) -> Result<Value, CoinGeckoPriceError> {
    let mut request = client.get(url);

    if let Some(api_key) = api_key {
        request = request.header(api_key_header(base_url), api_key);
    }

    request
        .send()
        .await
        .map_err(CoinGeckoPriceError::Request)?
        .error_for_status()
        .map_err(CoinGeckoPriceError::Request)?
        .json::<Value>()
        .await
        .map_err(CoinGeckoPriceError::Decode)
}

fn endpoint_url(base_url: &str, path: &str) -> Result<Url, CoinGeckoPriceError> {
    let url = format!("{}/{}", base_url.trim_end_matches('/'), path);
    Url::parse(&url).map_err(|e| CoinGeckoPriceError::InvalidBaseUrl(e.to_string()))
}

fn api_key_header(base_url: &str) -> &'static str {
    if base_url.contains("pro-api.coingecko.com") {
        "x-cg-pro-api-key"
    } else {
        "x-cg-demo-api-key"
    }
}

fn price_value_from_object(obj: &Map<String, Value>, currency: &str) -> Option<PriceValue> {
    let price = stringish_value(obj.get(currency)?)?;
    if price.trim().is_empty() {
        return None;
    }

    let change_key = format!("{currency}_24h_change");
    let price_change_percentage_24h = obj
        .get(&change_key)
        .and_then(stringish_value)
        .unwrap_or_default();

    Some(PriceValue {
        price,
        price_change_percentage_24h,
    })
}

fn stringish_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn contract_price_key(platform: &str, contract_address: &str) -> String {
    format!(
        "{}:{}",
        platform.trim().to_ascii_lowercase(),
        contract_address.trim().to_ascii_lowercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn price_value_parses_price_and_24h_change() {
        let raw = json!({
            "usd": 10.25,
            "usd_24h_change": -1.5
        });
        let value = price_value_from_object(raw.as_object().unwrap(), "usd").unwrap();

        assert_eq!(value.price, "10.25");
        assert_eq!(value.price_change_percentage_24h, "-1.5");
    }

    #[test]
    fn price_value_allows_missing_24h_change() {
        let raw = json!({ "usd": "0.998" });
        let value = price_value_from_object(raw.as_object().unwrap(), "usd").unwrap();

        assert_eq!(value.price, "0.998");
        assert_eq!(value.price_change_percentage_24h, "");
    }

    #[test]
    fn api_key_header_matches_demo_and_pro_hosts() {
        assert_eq!(
            api_key_header("https://api.coingecko.com/api/v3"),
            "x-cg-demo-api-key"
        );
        assert_eq!(
            api_key_header("https://pro-api.coingecko.com/api/v3"),
            "x-cg-pro-api-key"
        );
    }
}
