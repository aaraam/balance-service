use crate::db::models::{CryptoMarketPriceAssetDoc, CryptoMarketPriceDoc};
use bson::DateTime;
use serde::{Deserialize, Deserializer, Serialize};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TechbankMarketPriceError {
    #[error("invalid currency: {0}")]
    InvalidCurrency(String),

    #[error("invalid TechBank market price URL: {0}")]
    InvalidUrl(String),

    #[error("failed to build HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),

    #[error("TechBank market price request failed: {0}")]
    Request(#[source] reqwest::Error),

    #[error("TechBank market price response was not valid JSON: {0}")]
    Decode(#[source] reqwest::Error),
}

#[derive(Debug, Clone, Serialize)]
pub struct TechbankMarketPriceResponse {
    #[serde(rename = "value")]
    pub value: Vec<TechbankMarketPriceItem>,

    #[serde(rename = "Count")]
    pub count: usize,
}

impl<'de> Deserialize<'de> for TechbankMarketPriceResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = serde_json::Value::deserialize(deserializer)?;
        market_price_response_from_value(raw).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TechbankMarketPriceItem {
    pub id: String,
    pub symbol: String,

    #[serde(deserialize_with = "deserialize_stringish")]
    pub current_price: String,

    #[serde(deserialize_with = "deserialize_stringish")]
    pub price_change_percentage_24h: String,

    #[serde(default, deserialize_with = "deserialize_optional_stringish")]
    pub token_address: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FetchedMarketPrices {
    pub currency: String,
    pub source_url: String,
    pub upstream: TechbankMarketPriceResponse,
}

pub fn normalize_currency(input: &str) -> Result<String, TechbankMarketPriceError> {
    let currency = input.trim().to_ascii_lowercase();

    if currency.is_empty()
        || currency.len() > 16
        || !currency
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(TechbankMarketPriceError::InvalidCurrency(input.to_string()));
    }

    Ok(currency)
}

pub fn split_token_addresses(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn market_price_response_from_value(
    raw: serde_json::Value,
) -> Result<TechbankMarketPriceResponse, String> {
    match raw {
        serde_json::Value::Array(items) => {
            let value = deserialize_items(serde_json::Value::Array(items))?;
            Ok(TechbankMarketPriceResponse {
                count: value.len(),
                value,
            })
        }
        serde_json::Value::Object(mut obj) => {
            let items = obj
                .remove("value")
                .or_else(|| obj.remove("data"))
                .ok_or_else(|| "expected `value` array in object response".to_string())?;
            let value = deserialize_items(items)?;
            let count = obj
                .remove("Count")
                .or_else(|| obj.remove("count"))
                .map(parse_count)
                .transpose()?
                .unwrap_or(value.len());

            Ok(TechbankMarketPriceResponse { value, count })
        }
        other => Err(format!("expected array or object response, got {other}")),
    }
}

fn deserialize_items(items: serde_json::Value) -> Result<Vec<TechbankMarketPriceItem>, String> {
    serde_json::from_value(items).map_err(|e| format!("invalid market price items: {e}"))
}

fn parse_count(value: serde_json::Value) -> Result<usize, String> {
    match value {
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| "Count must be a non-negative integer".to_string()),
        serde_json::Value::String(s) => s
            .parse::<usize>()
            .map_err(|e| format!("Count must be a non-negative integer: {e}")),
        other => Err(format!("Count must be a number or string, got {other}")),
    }
}

pub async fn fetch_market_prices(
    base_url: &str,
    currency: &str,
    timeout_ms: u64,
) -> Result<FetchedMarketPrices, TechbankMarketPriceError> {
    let currency = normalize_currency(currency)?;
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|e| TechbankMarketPriceError::InvalidUrl(e.to_string()))?;
    url.query_pairs_mut()
        .clear()
        .append_pair("currency", &currency);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build()
        .map_err(TechbankMarketPriceError::ClientBuild)?;

    let response = client
        .get(url.clone())
        .send()
        .await
        .map_err(TechbankMarketPriceError::Request)?
        .error_for_status()
        .map_err(TechbankMarketPriceError::Request)?;

    let upstream = response
        .json::<TechbankMarketPriceResponse>()
        .await
        .map_err(TechbankMarketPriceError::Decode)?;

    Ok(FetchedMarketPrices {
        currency,
        source_url: url.to_string(),
        upstream,
    })
}

pub fn market_price_doc_from_fetch(fetch: &FetchedMarketPrices) -> CryptoMarketPriceDoc {
    let now = DateTime::now();
    let assets = fetch
        .upstream
        .value
        .iter()
        .map(|item| CryptoMarketPriceAssetDoc {
            id: item.id.clone(),
            symbol: item.symbol.clone(),
            current_price: item.current_price.clone(),
            price_change_percentage_24h: item.price_change_percentage_24h.clone(),
            token_address: item.token_address.clone(),
            token_addresses: split_token_addresses(item.token_address.as_deref()),
        })
        .collect::<Vec<_>>();

    CryptoMarketPriceDoc {
        currency: fetch.currency.clone(),
        source_url: fetch.source_url.clone(),
        source_count: fetch.upstream.count as i32,
        count: assets.len() as i32,
        assets,
        fetched_at: now,
        updated_at: now,
    }
}

fn deserialize_stringish<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;

    match value {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(serde::de::Error::custom(format!(
            "expected string or number, got {other}"
        ))),
    }
}

fn deserialize_optional_stringish<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;

    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) if s.trim().is_empty() => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s)),
        Some(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected string, number, or null, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_techbank_market_price_shape() {
        let raw = r#"{
            "value": [
                {
                    "id": "tether",
                    "symbol": "usdt",
                    "current_price": "0.9986780000",
                    "price_change_percentage_24h": "0.0225",
                    "token_address": "0xdac17f958d2ee523a2206206994597c13d831ec7, tr7nhqjekqxgtci8q8zy4pl8otszgjlj6t"
                },
                {
                    "id": "tron",
                    "symbol": "trx",
                    "current_price": 0.333162,
                    "price_change_percentage_24h": -1.97795,
                    "token_address": null
                }
            ],
            "Count": 2
        }"#;

        let parsed: TechbankMarketPriceResponse = serde_json::from_str(raw).unwrap();

        assert_eq!(parsed.count, 2);
        assert_eq!(parsed.value[0].current_price, "0.9986780000");
        assert_eq!(parsed.value[1].current_price, "0.333162");
        assert_eq!(parsed.value[1].token_address, None);
    }

    #[test]
    fn deserializes_raw_techbank_array_shape() {
        let raw = r#"[
            {
                "id": "ethereum",
                "symbol": "eth",
                "current_price": "1876.0200000000",
                "price_change_percentage_24h": "-5.30603",
                "token_address": null
            },
            {
                "id": "tether",
                "symbol": "usdt",
                "current_price": "0.9986550000",
                "price_change_percentage_24h": "0.01945",
                "token_address": "0xdac17,tr7nh"
            }
        ]"#;

        let parsed: TechbankMarketPriceResponse = serde_json::from_str(raw).unwrap();

        assert_eq!(parsed.count, 2);
        assert_eq!(parsed.value[0].id, "ethereum");
        assert_eq!(parsed.value[1].symbol, "usdt");
    }

    #[test]
    fn token_addresses_are_split_without_empty_values() {
        let addresses = split_token_addresses(Some(" 0xabc, ,TXYZ,"));
        assert_eq!(addresses, vec!["0xabc".to_string(), "TXYZ".to_string()]);
    }

    #[test]
    fn currency_is_normalized_and_limited() {
        assert_eq!(normalize_currency(" USD ").unwrap(), "usd");
        assert!(normalize_currency("").is_err());
        assert!(normalize_currency("usd?x=1").is_err());
    }
}
