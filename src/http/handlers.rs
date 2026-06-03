use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::core::token_decimals::{
    fetch_token_decimals, normalize_token_decimals_target, TokenDecimalsValidationError,
};
use crate::core::usd_value::enrich_balance_result_with_usd;
use crate::db::models::{CryptoMarketPriceAssetDoc, CryptoMarketTrackedTokenDoc};
use crate::db::{
    crypto_market_prices, crypto_market_tracked_tokens, refresh_jobs, snapshots,
    token_decimals_cache,
};
use crate::http::dto::{
    zero_result_from_request, AddCryptoMarketTokenRequest, AddCryptoMarketTokenResponse,
    BalanceRequest, BalanceResponse, CryptoMarketPriceQuery, CryptoMarketPriceResponse,
    CryptoMarketTrackedTokenDto, TokenDecimalsRequest, TokenDecimalsResponse,
};
use crate::http::error::ApiErrorBody;
use crate::http::validate::{validate_normalized_request, validate_request_limits};
use crate::market::service::{refresh_and_store_market_prices, MarketPriceRefreshError};
use crate::market::techbank::{
    normalize_currency, TechbankMarketPriceError, TechbankMarketPriceItem,
};
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bson::DateTime;
use serde_json::json;
use std::collections::HashSet;

pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

fn market_price_error_response(
    status: StatusCode,
    currency: String,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> Response {
    (
        status,
        Json(CryptoMarketPriceResponse {
            status: false,
            currency,
            source_url: String::new(),
            saved_to_db: false,
            fetched_at: String::new(),
            value: vec![],
            count: 0,
            source_count: 0,
            tracked_token_count: None,
            coingecko_updated_count: None,
            coingecko_error: None,
            error: Some(ApiErrorBody {
                code: code.into(),
                message: message.into(),
                details,
            }),
        }),
    )
        .into_response()
}

pub async fn refresh_crypto_market_prices(
    State(state): State<AppState>,
    Query(query): Query<CryptoMarketPriceQuery>,
) -> impl IntoResponse {
    let requested_currency = query.currency.unwrap_or_else(|| "usd".to_string());

    let outcome = match refresh_and_store_market_prices(&state, &requested_currency).await {
        Ok(outcome) => outcome,
        Err(MarketPriceRefreshError::Techbank(
            e @ TechbankMarketPriceError::InvalidCurrency(_),
        )) => {
            return market_price_error_response(
                StatusCode::BAD_REQUEST,
                requested_currency,
                "INVALID_CURRENCY",
                e.to_string(),
                None,
            );
        }
        Err(MarketPriceRefreshError::Techbank(e)) => {
            tracing::error!(error = %e, "crypto market price refresh failed");
            return market_price_error_response(
                StatusCode::BAD_GATEWAY,
                requested_currency,
                "CRYPTO_MARKET_PRICE_REFRESH_FAILED",
                e.to_string(),
                None,
            );
        }
        Err(MarketPriceRefreshError::TrackedTokenRead(e)) => {
            tracing::error!(error = %e, "failed to read tracked market-price tokens");
            return market_price_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                requested_currency,
                "CRYPTO_MARKET_TRACKED_TOKEN_DB_READ_FAILED",
                e.to_string(),
                None,
            );
        }
        Err(MarketPriceRefreshError::PriceWrite(e)) => {
            tracing::error!(error = %e, "failed to persist crypto market prices");
            return market_price_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                requested_currency,
                "CRYPTO_MARKET_PRICE_DB_WRITE_FAILED",
                e.to_string(),
                None,
            );
        }
    };

    (
        StatusCode::OK,
        Json(CryptoMarketPriceResponse {
            status: true,
            currency: outcome.snapshot.currency,
            source_url: outcome.snapshot.source_url,
            saved_to_db: true,
            fetched_at: outcome.snapshot.fetched_at.to_string(),
            value: market_price_items_from_assets(&outcome.snapshot.assets),
            count: outcome.snapshot.count as usize,
            source_count: outcome.upstream.count,
            tracked_token_count: Some(outcome.tracked_token_count),
            coingecko_updated_count: Some(outcome.coingecko_updated_count),
            coingecko_error: outcome.coingecko_error,
            error: None,
        }),
    )
        .into_response()
}

fn market_price_items_from_assets(
    assets: &[CryptoMarketPriceAssetDoc],
) -> Vec<TechbankMarketPriceItem> {
    assets
        .iter()
        .map(|asset| TechbankMarketPriceItem {
            id: asset.id.clone(),
            symbol: asset.symbol.clone(),
            current_price: asset.current_price.clone(),
            price_change_percentage_24h: asset.price_change_percentage_24h.clone(),
            token_address: asset.token_address.clone(),
        })
        .collect()
}

fn add_market_token_error_response(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> Response {
    (
        status,
        Json(AddCryptoMarketTokenResponse {
            status: false,
            token: None,
            price_updated: false,
            error: Some(ApiErrorBody {
                code: code.into(),
                message: message.into(),
                details,
            }),
        }),
    )
        .into_response()
}

pub async fn add_crypto_market_tracked_token(
    State(state): State<AppState>,
    Json(req): Json<AddCryptoMarketTokenRequest>,
) -> impl IntoResponse {
    let currency = match normalize_currency(req.currency.as_deref().unwrap_or("usd")) {
        Ok(currency) => currency,
        Err(e) => {
            return add_market_token_error_response(
                StatusCode::BAD_REQUEST,
                "INVALID_CURRENCY",
                e.to_string(),
                None,
            );
        }
    };

    let coingecko_id = match required_trimmed(&req.coingecko_id, "coingeckoId") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let symbol = match required_trimmed(&req.symbol, "symbol") {
        Ok(value) => value.to_ascii_lowercase(),
        Err(response) => return response,
    };
    let asset_platform_id =
        optional_trimmed(req.asset_platform_id.as_deref()).map(|value| value.to_ascii_lowercase());
    let contract_address = optional_trimmed(req.contract_address.as_deref());

    if contract_address.is_some() && asset_platform_id.is_none() {
        return add_market_token_error_response(
            StatusCode::BAD_REQUEST,
            "ASSET_PLATFORM_REQUIRED",
            "assetPlatformId is required when contractAddress is provided",
            Some(json!({ "contractAddress": contract_address })),
        );
    }

    let token_addresses =
        normalized_token_addresses(contract_address.as_deref(), &req.token_addresses);
    let now = DateTime::now();
    let token = CryptoMarketTrackedTokenDoc {
        tracking_key: tracked_token_key(
            &coingecko_id,
            asset_platform_id.as_deref(),
            contract_address.as_deref(),
        ),
        currency: currency.clone(),
        coingecko_id,
        symbol,
        asset_platform_id,
        contract_address,
        token_addresses,
        enabled: true,
        created_at: now,
        updated_at: now,
    };

    let saved = match crypto_market_tracked_tokens::upsert_token(&state.mongo.db, &token).await {
        Ok(saved) => saved,
        Err(e) => {
            tracing::error!(error = %e, "failed to save tracked market-price token");
            return add_market_token_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "CRYPTO_MARKET_TRACKED_TOKEN_DB_WRITE_FAILED",
                e.to_string(),
                None,
            );
        }
    };

    let mut refresh_error = None;
    let price_updated = match refresh_and_store_market_prices(&state, &currency).await {
        Ok(outcome) => outcome
            .snapshot
            .assets
            .iter()
            .any(|asset| asset_matches_tracked_token(asset, &saved)),
        Err(e) => {
            tracing::warn!(
                currency = %currency,
                tracking_key = %saved.tracking_key,
                error = %e,
                "tracked token saved but immediate market-price refresh failed"
            );
            refresh_error = Some(e.to_string());
            false
        }
    };

    (
        StatusCode::OK,
        Json(AddCryptoMarketTokenResponse {
            status: true,
            token: Some(CryptoMarketTrackedTokenDto::from(&saved)),
            price_updated,
            error: refresh_error.map(|message| ApiErrorBody {
                code: "CRYPTO_MARKET_PRICE_REFRESH_FAILED".to_string(),
                message,
                details: Some(json!({ "tokenSaved": true })),
            }),
        }),
    )
        .into_response()
}

fn required_trimmed(value: &str, field: &str) -> Result<String, Response> {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        return Err(add_market_token_error_response(
            StatusCode::BAD_REQUEST,
            "MISSING_REQUIRED_FIELD",
            format!("{field} is required"),
            Some(json!({ "field": field })),
        ));
    }

    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
    {
        return Err(add_market_token_error_response(
            StatusCode::BAD_REQUEST,
            "INVALID_FIELD",
            format!("{field} contains unsupported characters"),
            Some(json!({ "field": field })),
        ));
    }

    Ok(trimmed.to_string())
}

fn optional_trimmed(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn normalized_token_addresses(
    contract_address: Option<&str>,
    token_addresses: &[String],
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for address in contract_address
        .into_iter()
        .chain(token_addresses.iter().map(String::as_str))
    {
        let address = address.trim();
        if address.is_empty() {
            continue;
        }

        if seen.insert(address.to_ascii_lowercase()) {
            out.push(address.to_string());
        }
    }

    out
}

fn tracked_token_key(
    coingecko_id: &str,
    asset_platform_id: Option<&str>,
    contract_address: Option<&str>,
) -> String {
    match (asset_platform_id, contract_address) {
        (Some(platform), Some(contract)) => {
            format!(
                "contract:{}:{}",
                platform.trim().to_ascii_lowercase(),
                contract.trim().to_ascii_lowercase()
            )
        }
        _ => format!("id:{}", coingecko_id.trim().to_ascii_lowercase()),
    }
}

fn asset_matches_tracked_token(
    asset: &CryptoMarketPriceAssetDoc,
    token: &CryptoMarketTrackedTokenDoc,
) -> bool {
    if asset.id.eq_ignore_ascii_case(&token.coingecko_id) {
        return true;
    }

    token.token_addresses.iter().any(|tracked_address| {
        asset
            .token_addresses
            .iter()
            .any(|asset_address| asset_address.eq_ignore_ascii_case(tracked_address))
            || asset
                .token_address
                .as_deref()
                .is_some_and(|asset_address| asset_address.eq_ignore_ascii_case(tracked_address))
    })
}

fn token_decimals_error_response(
    status: StatusCode,
    blockchain: impl Into<String>,
    contract_address: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> Response {
    (
        status,
        Json(TokenDecimalsResponse {
            status: false,
            blockchain: blockchain.into(),
            contract_address: contract_address.into(),
            exists: false,
            decimals: None,
            error: Some(ApiErrorBody {
                code: code.into(),
                message: message.into(),
                details,
            }),
        }),
    )
        .into_response()
}

pub async fn get_token_decimals(
    State(state): State<AppState>,
    Json(req): Json<TokenDecimalsRequest>,
) -> impl IntoResponse {
    let target = match normalize_token_decimals_target(&req.blockchain, &req.contract_address) {
        Ok(target) => target,
        Err(e) => {
            let (code, details) = match &e {
                TokenDecimalsValidationError::UnsupportedBlockchain(blockchain) => (
                    "INVALID_BLOCKCHAIN",
                    Some(json!({ "blockchain": blockchain })),
                ),
                TokenDecimalsValidationError::InvalidContractAddress {
                    blockchain,
                    contract_address,
                } => (
                    "INVALID_CONTRACT",
                    Some(json!({
                        "blockchain": blockchain,
                        "contractAddress": contract_address
                    })),
                ),
            };

            return token_decimals_error_response(
                StatusCode::BAD_REQUEST,
                req.blockchain,
                req.contract_address,
                code,
                e.to_string(),
                details,
            );
        }
    };

    match token_decimals_cache::get_cached(
        &state.mongo.db,
        &target.blockchain,
        &target.contract_address,
    )
    .await
    {
        Ok(Some(cached)) => {
            return (
                StatusCode::OK,
                Json(TokenDecimalsResponse {
                    status: true,
                    blockchain: target.blockchain,
                    contract_address: target.contract_address,
                    exists: cached.exists,
                    decimals: cached.decimals,
                    error: None,
                }),
            )
                .into_response();
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(
                blockchain = %target.blockchain,
                contract = %target.contract_address,
                error = %e,
                "token decimals cache read failed; falling back to RPC"
            );
        }
    }

    match fetch_token_decimals(&state.cfg, &target).await {
        Ok(decimals) => {
            if let Err(e) = token_decimals_cache::upsert(
                &state.mongo.db,
                &target.blockchain,
                &target.contract_address,
                decimals,
            )
            .await
            {
                tracing::warn!(
                    blockchain = %target.blockchain,
                    contract = %target.contract_address,
                    error = %e,
                    "token decimals cache write failed"
                );
            }

            (
                StatusCode::OK,
                Json(TokenDecimalsResponse {
                    status: true,
                    blockchain: target.blockchain,
                    contract_address: target.contract_address,
                    exists: decimals.is_some(),
                    decimals,
                    error: None,
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(
                blockchain = %target.blockchain,
                contract = %target.contract_address,
                error = %e,
                "token decimals lookup failed"
            );
            token_decimals_error_response(
                StatusCode::BAD_GATEWAY,
                target.blockchain,
                target.contract_address,
                "TOKEN_DECIMALS_LOOKUP_FAILED",
                e.to_string(),
                None,
            )
        }
    }
}

fn cache_key_request(req: &BalanceRequest) -> BalanceRequest {
    let mut key_req = req.clone();
    key_req.hard_refresh = false;
    key_req
}

fn should_refresh_existing(hard_refresh: bool, existing_is_complete: bool) -> bool {
    hard_refresh || !existing_is_complete
}

async fn build_balance_response(
    state: &AppState,
    req: BalanceRequest,
) -> Result<BalanceResponse, Response> {
    if let Err(e) = validate_request_limits(&req) {
        return Err(e.into_response());
    }

    let normalized = normalize_request(&req);
    if let Err(e) = validate_normalized_request(&normalized) {
        return Err(e.into_response());
    }

    let canonical_json = match serde_json::to_string(&cache_key_request(&normalized)) {
        Ok(s) => s,
        Err(e) => {
            return Err(crate::http::error::ApiError::bad_request(
                "JSON_ERROR",
                &e.to_string(),
                None,
            )
            .into_response());
        }
    };

    let request_key = request_key_from_canonical_json(&canonical_json);
    let zero_result = zero_result_from_request(&normalized);

    let snap = snapshots::get_snapshot(&state.mongo.db, &request_key).await;

    match snap {
        Ok(Some(existing)) => {
            let existing_is_complete = existing.is_complete;
            let should_refresh =
                should_refresh_existing(normalized.hard_refresh, existing_is_complete);

            if should_refresh {
                if let Ok(did_queue) =
                    refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await
                {
                    if did_queue {
                        let _ =
                            snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued")
                                .await;
                        if let Err(e) = state.queue.publish(&request_key).await {
                            tracing::error!(request_key=%request_key, error=%e, "failed to publish job to queue");
                        }
                    }
                }
            }

            Ok(BalanceResponse {
                status: true,
                is_complete: existing_is_complete,
                has_changed: existing.has_changed,
                result: existing.result,
                request_key,
                progress_stage: existing.progress_stage,
                error: None,
            })
        }

        _ => {
            let normalized_value =
                serde_json::to_value(&normalized).unwrap_or(serde_json::json!({}));

            let _ = snapshots::upsert_empty_snapshot(
                &state.mongo.db,
                &request_key,
                normalized_value,
                zero_result.clone(),
            )
            .await;

            if let Ok(did_queue) =
                refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await
            {
                if did_queue {
                    let _ =
                        snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued").await;
                    if let Err(e) = state.queue.publish(&request_key).await {
                        tracing::error!(request_key=%request_key, error=%e, "failed to publish job to queue");
                    }
                }
            }

            Ok(BalanceResponse {
                status: true,
                is_complete: false,
                has_changed: false,
                result: zero_result,
                request_key,
                progress_stage: Some("queued".to_string()),
                error: None,
            })
        }
    }
}

pub async fn get_job_status(
    State(state): State<AppState>,
    Path(request_key): Path<String>,
) -> impl IntoResponse {
    // Use lightweight projection to avoid fetching the massive result JSON
    let snap = snapshots::get_snapshot_status(&state.mongo.db, &request_key).await;

    match snap {
        Ok(Some(s)) => (
            StatusCode::OK,
            Json(BalanceResponse {
                status: true,
                is_complete: s.is_complete,
                has_changed: s.has_changed,
                request_key,
                result: serde_json::json!({}), // Return empty result to avoid heavy payloads
                progress_stage: s.progress_stage,
                error: None,
            }),
        )
            .into_response(),

        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "status": false,
                "message": "request_key not found"
            })),
        )
            .into_response(),
    }
}

pub async fn get_multi_wallet_balances(
    State(state): State<AppState>,
    Json(req): Json<BalanceRequest>,
) -> impl IntoResponse {
    match build_balance_response(&state, req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(response) => response,
    }
}

fn balance_usd_error_response(
    status: StatusCode,
    request_key: String,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> Response {
    (
        status,
        Json(BalanceResponse {
            status: false,
            is_complete: false,
            has_changed: false,
            request_key,
            result: serde_json::json!({}),
            progress_stage: None,
            error: Some(ApiErrorBody {
                code: code.into(),
                message: message.into(),
                details,
            }),
        }),
    )
        .into_response()
}

pub async fn get_multi_wallet_balances_usd(
    State(state): State<AppState>,
    Json(req): Json<BalanceRequest>,
) -> impl IntoResponse {
    let balance_response = match build_balance_response(&state, req).await {
        Ok(response) => response,
        Err(response) => return response,
    };

    let prices = match crypto_market_prices::get_latest(&state.mongo.db, "usd").await {
        Ok(Some(prices)) => prices,
        Ok(None) => {
            return balance_usd_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                balance_response.request_key,
                "MARKET_PRICES_NOT_FOUND",
                "USD market prices are not available in the database",
                Some(json!({ "collection": "crypto_market_prices", "currency": "usd" })),
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to load USD market prices");
            return balance_usd_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                balance_response.request_key,
                "MARKET_PRICES_DB_READ_FAILED",
                e.to_string(),
                None,
            );
        }
    };

    let enriched = enrich_balance_result_with_usd(&balance_response.result, &prices);

    (
        StatusCode::OK,
        Json(BalanceResponse {
            status: true,
            is_complete: balance_response.is_complete,
            has_changed: balance_response.has_changed,
            request_key: balance_response.request_key,
            result: enriched.result,
            progress_stage: balance_response.progress_stage,
            error: None,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::dto::ContractGroup;

    #[test]
    fn cache_key_ignores_hard_refresh_flag() {
        let mut req = BalanceRequest {
            hard_refresh: true,
            contracts: vec![ContractGroup {
                network_name: "trx".to_string(),
                contract_addresses: vec!["token".to_string()],
            }],
            wallet_addresses: vec!["wallet".to_string()],
            solana_wallet_addresses: vec![],
            tron_wallet_addresses: vec![],
            doge_wallet_addresses: vec![],
            btc_wallet_addresses: vec![],
        };

        let hard_refresh_key = serde_json::to_string(&cache_key_request(&req)).unwrap();
        req.hard_refresh = false;
        let normal_key = serde_json::to_string(&cache_key_request(&req)).unwrap();

        assert_eq!(hard_refresh_key, normal_key);
    }

    #[test]
    fn hard_refresh_requeues_complete_snapshots() {
        assert!(should_refresh_existing(true, true));
        assert!(should_refresh_existing(false, false));
        assert!(!should_refresh_existing(false, true));
    }
}
