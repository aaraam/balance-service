use crate::db::models::{CryptoMarketPriceAssetDoc, CryptoMarketPriceDoc};
use bigdecimal::BigDecimal;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::str::FromStr;

const MISSING_VALUE_PLACEHOLDER: &str = "- -";

#[derive(Debug, Clone)]
struct PricePoint {
    price: BigDecimal,
    price_raw: String,
    price_change_percentage_24h: String,
}

#[derive(Debug, Clone)]
struct PriceIndex {
    by_symbol: HashMap<String, PricePoint>,
    by_address: HashMap<String, PricePoint>,
}

#[derive(Debug, Clone)]
struct EnrichedBalance {
    value: Value,
    usd_value: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct BalanceUsdEnrichment {
    pub result: Value,
}

impl PriceIndex {
    fn from_doc(doc: &CryptoMarketPriceDoc) -> Self {
        let mut by_symbol = HashMap::new();
        let mut by_address = HashMap::new();

        for asset in &doc.assets {
            let Ok(price) = BigDecimal::from_str(asset.current_price.trim()) else {
                continue;
            };

            let point = PricePoint {
                price,
                price_raw: display_market_value(&asset.current_price),
                price_change_percentage_24h: display_market_value(
                    &asset.price_change_percentage_24h,
                ),
            };

            by_symbol
                .entry(normalize_lookup_key(&asset.symbol))
                .or_insert_with(|| point.clone());

            for address in token_addresses(asset) {
                by_address
                    .entry(normalize_lookup_key(&address))
                    .or_insert_with(|| point.clone());
            }
        }

        Self {
            by_symbol,
            by_address,
        }
    }

    fn get(&self, token: &str) -> Option<&PricePoint> {
        let key = normalize_lookup_key(token);

        self.by_address
            .get(&key)
            .or_else(|| self.by_symbol.get(&key))
    }
}

pub fn enrich_balance_result_with_usd(
    balance_result: &Value,
    prices: &CryptoMarketPriceDoc,
) -> BalanceUsdEnrichment {
    let price_index = PriceIndex::from_doc(prices);

    let data = balance_result
        .get("data")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| enrich_wallet_row(row, &price_index))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let total_balance = balance_result
        .get("total")
        .and_then(|total| total.get("balance"))
        .unwrap_or(&Value::Null);

    let enriched_total_balance = enrich_balance_object(total_balance, &price_index);

    BalanceUsdEnrichment {
        result: json!({
            "data": data,
            "total": {
                "balance": enriched_total_balance.value,
                "usdValue": format_decimal(&enriched_total_balance.usd_value)
            }
        }),
    }
}

fn enrich_wallet_row(row: &Value, price_index: &PriceIndex) -> Value {
    let balance = row.get("balance").unwrap_or(&Value::Null);
    let enriched_balance = enrich_balance_object(balance, price_index);
    let wallet_address = row
        .get("walletAddress")
        .cloned()
        .unwrap_or_else(|| json!(""));

    json!({
        "walletAddress": wallet_address,
        "balance": enriched_balance.value,
        "usdValue": format_decimal(&enriched_balance.usd_value)
    })
}

fn enrich_balance_object(balance: &Value, price_index: &PriceIndex) -> EnrichedBalance {
    let mut out = Map::new();
    let mut total_usd = BigDecimal::from(0);

    let Some(networks) = balance.as_object() else {
        return EnrichedBalance {
            value: Value::Object(out),
            usd_value: total_usd,
        };
    };

    for (network, tokens) in networks {
        let mut token_out = Map::new();

        if let Some(token_map) = tokens.as_object() {
            for (token, amount_value) in token_map {
                let amount_raw = amount_as_string(amount_value);
                let amount =
                    BigDecimal::from_str(&amount_raw).unwrap_or_else(|_| BigDecimal::from(0));

                let (price, price_change, usd_value) = match price_index.get(token) {
                    Some(price) => {
                        let usd_value = &amount * &price.price;
                        total_usd += usd_value.clone();

                        (
                            Value::String(price.price_raw.clone()),
                            Value::String(price.price_change_percentage_24h.clone()),
                            Value::String(format_decimal(&usd_value)),
                        )
                    }
                    None => (
                        Value::String(MISSING_VALUE_PLACEHOLDER.to_string()),
                        Value::String(MISSING_VALUE_PLACEHOLDER.to_string()),
                        Value::String(MISSING_VALUE_PLACEHOLDER.to_string()),
                    ),
                };

                token_out.insert(
                    token.clone(),
                    json!({
                        "amount": amount_raw,
                        "price": price,
                        "usdValue": usd_value,
                        "priceChangePercentage24h": price_change
                    }),
                );
            }
        }

        out.insert(network.clone(), Value::Object(token_out));
    }

    EnrichedBalance {
        value: Value::Object(out),
        usd_value: total_usd,
    }
}

fn token_addresses(asset: &CryptoMarketPriceAssetDoc) -> Vec<String> {
    if !asset.token_addresses.is_empty() {
        return asset.token_addresses.clone();
    }

    asset
        .token_address
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_lookup_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn amount_as_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => "0".to_string(),
    }
}

fn format_decimal(value: &BigDecimal) -> String {
    let normalized = value.normalized().to_string();

    if normalized == "0" || normalized == "0E-18" {
        return "0".to_string();
    }

    normalized
}

fn display_market_value(value: &str) -> String {
    let trimmed = value.trim();

    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return MISSING_VALUE_PLACEHOLDER.to_string();
    }

    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::DateTime;

    fn price_doc() -> CryptoMarketPriceDoc {
        CryptoMarketPriceDoc {
            currency: "usd".to_string(),
            source_url: "https://api.techbank.live/v2/crypto-market-price?currency=usd".to_string(),
            source_count: 3,
            count: 3,
            assets: vec![
                CryptoMarketPriceAssetDoc {
                    id: "ethereum".to_string(),
                    symbol: "eth".to_string(),
                    current_price: "2000".to_string(),
                    price_change_percentage_24h: "-1.5".to_string(),
                    token_address: None,
                    token_addresses: vec![],
                },
                CryptoMarketPriceAssetDoc {
                    id: "tether".to_string(),
                    symbol: "usdt".to_string(),
                    current_price: "1".to_string(),
                    price_change_percentage_24h: "0.01".to_string(),
                    token_address: Some("0xdac17f958d2ee523a2206206994597c13d831ec7".to_string()),
                    token_addresses: vec!["0xdac17f958d2ee523a2206206994597c13d831ec7".to_string()],
                },
                CryptoMarketPriceAssetDoc {
                    id: "tron".to_string(),
                    symbol: "trx".to_string(),
                    current_price: "0.25".to_string(),
                    price_change_percentage_24h: "".to_string(),
                    token_address: None,
                    token_addresses: vec![],
                },
            ],
            fetched_at: DateTime::now(),
            updated_at: DateTime::now(),
        }
    }

    #[test]
    fn enriches_wallet_tokens_and_totals_with_usd_without_extra_grouping() {
        let result = json!({
            "data": [
                {
                    "walletAddress": "0xabc",
                    "balance": {
                        "eth": {
                            "eth": "2.000000000000000000",
                            "0xdac17f958d2ee523a2206206994597c13d831ec7": "5.000000000000000000"
                        },
                        "trx": {
                            "trx": "10.000000000000000000"
                        }
                    }
                }
            ],
            "total": {
                "balance": {
                    "eth": {
                        "eth": "2.000000000000000000",
                        "0xdac17f958d2ee523a2206206994597c13d831ec7": "5.000000000000000000"
                    },
                    "trx": {
                        "trx": "10.000000000000000000"
                    }
                }
            }
        });

        let enriched = enrich_balance_result_with_usd(&result, &price_doc());

        assert_eq!(enriched.result["data"][0]["usdValue"], json!("4007.5"));
        assert_eq!(enriched.result["total"]["usdValue"], json!("4007.5"));
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["eth"]["amount"],
            json!("2.000000000000000000")
        );
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["eth"]["price"],
            json!("2000")
        );
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["eth"]["usdValue"],
            json!("4000")
        );
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["eth"]["priceChangePercentage24h"],
            json!("-1.5")
        );
        assert_eq!(
            enriched.result["data"][0]["balance"]["trx"]["trx"]["priceChangePercentage24h"],
            json!("- -")
        );
        assert!(enriched.result["data"][0]["balance"]["eth"]
            .get("tokens")
            .is_none());
    }

    #[test]
    fn missing_prices_keep_the_same_token_key_with_placeholders() {
        let result = json!({
            "data": [
                {
                    "walletAddress": "0xabc",
                    "balance": {
                        "eth": {
                            "unknown": "5.000000000000000000",
                            "eth": "1.000000000000000000"
                        }
                    }
                }
            ],
            "total": {
                "balance": {
                    "eth": {
                        "unknown": "5.000000000000000000",
                        "eth": "1.000000000000000000"
                    }
                }
            }
        });

        let enriched = enrich_balance_result_with_usd(&result, &price_doc());

        assert_eq!(enriched.result["total"]["usdValue"], json!("2000"));
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["unknown"]["amount"],
            json!("5.000000000000000000")
        );
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["unknown"]["price"],
            json!("- -")
        );
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["unknown"]["usdValue"],
            json!("- -")
        );
        assert_eq!(
            enriched.result["data"][0]["balance"]["eth"]["unknown"]["priceChangePercentage24h"],
            json!("- -")
        );
    }
}
