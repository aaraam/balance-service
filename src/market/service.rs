use crate::db::models::CryptoMarketPriceAssetDoc;
use crate::db::{crypto_market_prices, crypto_market_tracked_tokens};
use crate::market::coingecko::fetch_tracked_token_prices;
use crate::market::techbank::{
    fetch_market_prices, market_price_doc_from_fetch, TechbankMarketPriceError,
    TechbankMarketPriceResponse,
};
use crate::AppState;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tokio::time::MissedTickBehavior;

#[derive(Debug, Error)]
pub enum MarketPriceRefreshError {
    #[error(transparent)]
    Techbank(#[from] TechbankMarketPriceError),

    #[error("failed to read tracked market-price tokens: {0}")]
    TrackedTokenRead(#[source] mongodb::error::Error),

    #[error("failed to persist crypto market prices: {0}")]
    PriceWrite(#[source] mongodb::error::Error),
}

#[derive(Debug, Clone)]
pub struct MarketPriceRefreshOutcome {
    pub snapshot: crate::db::models::CryptoMarketPriceDoc,
    pub upstream: TechbankMarketPriceResponse,
    pub tracked_token_count: usize,
    pub coingecko_updated_count: usize,
    pub coingecko_error: Option<String>,
}

pub async fn refresh_and_store_market_prices(
    state: &AppState,
    requested_currency: &str,
) -> Result<MarketPriceRefreshOutcome, MarketPriceRefreshError> {
    let fetched = fetch_market_prices(
        &state.cfg.crypto_market_price_url,
        requested_currency,
        state.cfg.rpc_timeout_ms,
    )
    .await?;

    let mut snapshot = market_price_doc_from_fetch(&fetched);
    let tracked_tokens =
        crypto_market_tracked_tokens::list_enabled_by_currency(&state.mongo.db, &snapshot.currency)
            .await
            .map_err(MarketPriceRefreshError::TrackedTokenRead)?;

    let mut coingecko_updated_count = 0;
    let mut coingecko_error = None;

    if !tracked_tokens.is_empty() {
        match fetch_tracked_token_prices(
            &state.cfg.coingecko_api_base_url,
            state.cfg.coingecko_api_key.as_deref(),
            &snapshot.currency,
            &tracked_tokens,
            state.cfg.rpc_timeout_ms,
        )
        .await
        {
            Ok(coingecko_assets) => {
                coingecko_updated_count = coingecko_assets.len();
                merge_market_price_assets(&mut snapshot.assets, coingecko_assets);
                snapshot.count = snapshot.assets.len() as i32;
            }
            Err(e) => {
                tracing::warn!(
                    currency = %snapshot.currency,
                    tracked_token_count = tracked_tokens.len(),
                    error = %e,
                    "CoinGecko tracked token price refresh failed; keeping TechBank base prices"
                );
                coingecko_error = Some(e.to_string());
            }
        }
    }

    crypto_market_prices::upsert_latest(&state.mongo.db, &snapshot)
        .await
        .map_err(MarketPriceRefreshError::PriceWrite)?;

    Ok(MarketPriceRefreshOutcome {
        snapshot,
        upstream: fetched.upstream,
        tracked_token_count: tracked_tokens.len(),
        coingecko_updated_count,
        coingecko_error,
    })
}

pub async fn run_market_price_refresher(state: AppState) {
    let interval_secs = state.cfg.crypto_market_price_refresh_interval_secs;

    if interval_secs == 0 {
        tracing::info!("crypto market price refresher disabled");
        return;
    }

    tracing::info!(
        interval_secs,
        currency = "usd",
        "crypto market price refresher started"
    );

    refresh_once(&state, "startup").await;

    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        refresh_once(&state, "interval").await;
    }
}

async fn refresh_once(state: &AppState, trigger: &str) {
    match refresh_and_store_market_prices(state, "usd").await {
        Ok(outcome) => {
            tracing::info!(
                trigger,
                currency = %outcome.snapshot.currency,
                count = outcome.snapshot.count,
                tracked_token_count = outcome.tracked_token_count,
                coingecko_updated_count = outcome.coingecko_updated_count,
                coingecko_error = outcome.coingecko_error.as_deref().unwrap_or(""),
                "crypto market prices refreshed"
            );
        }
        Err(e) => {
            tracing::error!(
                trigger,
                error = %e,
                "crypto market price scheduled refresh failed"
            );
        }
    }
}

pub fn merge_market_price_assets(
    existing: &mut Vec<CryptoMarketPriceAssetDoc>,
    updates: Vec<CryptoMarketPriceAssetDoc>,
) {
    let mut index = HashMap::new();

    for (idx, asset) in existing.iter().enumerate() {
        for key in asset_match_keys(asset) {
            index.entry(key).or_insert(idx);
        }
    }

    for update in updates {
        let existing_idx = asset_match_keys(&update)
            .into_iter()
            .find_map(|key| index.get(&key).copied());

        if let Some(idx) = existing_idx {
            existing[idx] = update.clone();
            for key in asset_match_keys(&update) {
                index.insert(key, idx);
            }
        } else {
            let idx = existing.len();
            for key in asset_match_keys(&update) {
                index.insert(key, idx);
            }
            existing.push(update);
        }
    }
}

fn asset_match_keys(asset: &CryptoMarketPriceAssetDoc) -> Vec<String> {
    let mut out = Vec::new();

    if !asset.id.trim().is_empty() {
        out.push(format!("id:{}", asset.id.trim().to_ascii_lowercase()));
    }

    for address in asset
        .token_addresses
        .iter()
        .map(String::as_str)
        .chain(asset.token_address.as_deref())
    {
        let address = address.trim();
        if !address.is_empty() {
            out.push(format!("address:{}", address.to_ascii_lowercase()));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(
        id: &str,
        symbol: &str,
        price: &str,
        address: Option<&str>,
    ) -> CryptoMarketPriceAssetDoc {
        CryptoMarketPriceAssetDoc {
            id: id.to_string(),
            symbol: symbol.to_string(),
            current_price: price.to_string(),
            price_change_percentage_24h: "1".to_string(),
            token_address: address.map(ToOwned::to_owned),
            token_addresses: address
                .map(|addr| vec![addr.to_string()])
                .unwrap_or_default(),
        }
    }

    #[test]
    fn merge_replaces_existing_assets_by_id() {
        let mut existing = vec![asset("ethereum", "eth", "100", None)];
        merge_market_price_assets(&mut existing, vec![asset("ethereum", "eth", "200", None)]);

        assert_eq!(existing.len(), 1);
        assert_eq!(existing[0].current_price, "200");
    }

    #[test]
    fn merge_replaces_existing_assets_by_address() {
        let mut existing = vec![asset("old-usdt", "usdt", "0.9", Some("0xABC"))];
        merge_market_price_assets(
            &mut existing,
            vec![asset("tether", "usdt", "1", Some("0xabc"))],
        );

        assert_eq!(existing.len(), 1);
        assert_eq!(existing[0].id, "tether");
        assert_eq!(existing[0].current_price, "1");
    }

    #[test]
    fn merge_appends_new_assets() {
        let mut existing = vec![asset("ethereum", "eth", "100", None)];
        merge_market_price_assets(&mut existing, vec![asset("new-token", "new", "1", None)]);

        assert_eq!(existing.len(), 2);
    }
}
