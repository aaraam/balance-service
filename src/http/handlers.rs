use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::core::token_decimals::{
    fetch_token_decimals, normalize_token_decimals_target, TokenDecimalsValidationError,
};
use crate::db::{refresh_jobs, snapshots};
use crate::http::dto::{
    zero_result_from_request, BalanceRequest, BalanceResponse, TokenDecimalsRequest,
    TokenDecimalsResponse,
};
use crate::http::error::ApiErrorBody;
use crate::http::validate::validate_request_limits;
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
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

    match fetch_token_decimals(&state.cfg, &target).await {
        Ok(decimals) => (
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
            .into_response(),
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
    if let Err(e) = validate_request_limits(&req) {
        return e.into_response();
    }

    let normalized = normalize_request(&req);

    let canonical_json = match serde_json::to_string(&cache_key_request(&normalized)) {
        Ok(s) => s,
        Err(e) => {
            return crate::http::error::ApiError::bad_request("JSON_ERROR", &e.to_string(), None)
                .into_response();
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

            (
                StatusCode::OK,
                Json(BalanceResponse {
                    status: true,
                    is_complete: existing_is_complete,
                    has_changed: existing.has_changed,
                    result: existing.result, // Main fetch still returns the full payload
                    request_key,
                    progress_stage: existing.progress_stage,
                    error: None,
                }),
            )
                .into_response()
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

            (
                StatusCode::OK,
                Json(BalanceResponse {
                    status: true,
                    is_complete: false,
                    has_changed: false,
                    result: zero_result,
                    request_key,
                    progress_stage: Some("queued".to_string()),
                    error: None,
                }),
            )
                .into_response()
        }
    }
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
