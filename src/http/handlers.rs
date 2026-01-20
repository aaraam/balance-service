// ==================================================
// balance-service\src\http\handlers.rs
// ==================================================

use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::db::{refresh_jobs, snapshots};
use crate::http::dto::{zero_result_from_request, BalanceRequest, BalanceResponse};
use crate::http::validate::{validate_normalized_request, validate_request_limits};
use crate::AppState;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use bson::DateTime;
use serde_json::json;

const STALE_AFTER_SECS: i64 = 30;

/// Hard refresh cooldown (prevents hammering)
const HARD_REFRESH_COOLDOWN_SECS: i64 = 10;

pub async fn health() -> &'static str {
    "ok"
}

pub async fn get_multi_wallet_balances(
    State(state): State<AppState>,
    Json(req): Json<BalanceRequest>,
) -> impl IntoResponse {
    // 1) Validate raw limits first (cheap anti-abuse)
    if let Err(e) = validate_request_limits(&req) {
        return e.into_response();
    }

    // 2) Normalize
    let normalized = normalize_request(&req);

    // 3) Validate normalized request (addresses, networks, work units)
    if let Err(e) = validate_normalized_request(&normalized) {
        return e.into_response();
    }

    let canonical = serde_json::to_string(&normalized).unwrap_or_else(|_| "{}".to_string());
    let request_key = request_key_from_canonical_json(&canonical);

    tracing::info!(
        request_key = %request_key,
        hard_refresh = normalized.hard_refresh,
        evm_wallets = normalized.wallet_addresses.len(),
        sol_wallets = normalized.solana_wallet_addresses.len(),
        tron_wallets = normalized.tron_wallet_addresses.len(),
        doge_wallets = normalized.doge_wallet_addresses.len(),
        btc_wallets = normalized.btc_wallet_addresses.len(),
        contracts = ?normalized.contracts
            .iter()
            .map(|c| (&c.network_name, c.contract_addresses.len()))
            .collect::<Vec<_>>(),
        "balances request received"
    );

    let now = DateTime::now();

    match snapshots::get_snapshot(&state.mongo.db, &request_key).await {
        Ok(Some(doc)) => {
            let age_secs = (now.timestamp_millis() - doc.last_updated_at.timestamp_millis()) / 1000;
            let is_stale = age_secs > STALE_AFTER_SECS;

            tracing::debug!(
                request_key = %request_key,
                age_secs = age_secs,
                is_stale = is_stale,
                refresh_state = %doc.refresh_state,
                is_complete = doc.is_complete,
                "snapshot hit"
            );

            // Hard refresh cooldown to prevent spam
            let hard_refresh_allowed = if normalized.hard_refresh {
                age_secs >= HARD_REFRESH_COOLDOWN_SECS
            } else {
                true
            };

            if (is_stale || normalized.hard_refresh) && hard_refresh_allowed {
                match refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await {
                    Ok(did_queue) => {
                        tracing::info!(
                            request_key = %request_key,
                            did_queue = did_queue,
                            "refresh job enqueue_or_requeue result"
                        );

                        if did_queue {
                            let _ = snapshots::set_refresh_state(
                                &state.mongo.db,
                                &request_key,
                                "queued",
                            )
                            .await;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            request_key=%request_key,
                            error=%e,
                            "failed to enqueue refresh job"
                        );
                    }
                }
            } else if normalized.hard_refresh && !hard_refresh_allowed {
                tracing::debug!(
                    request_key = %request_key,
                    age_secs = age_secs,
                    cooldown_secs = HARD_REFRESH_COOLDOWN_SECS,
                    "hard_refresh skipped due to cooldown"
                );
            }

            Json(BalanceResponse {
                status: true,
                is_complete: doc.is_complete,
                has_changed: doc.has_changed, // ✅ NEW: Return from DB
                result: doc.result,
                error: None,
            })
            .into_response()
        }

        Ok(None) => {
            tracing::info!(
                request_key=%request_key,
                "snapshot miss → creating zero snapshot + enqueue job"
            );

            let normalized_json = serde_json::to_value(&normalized).unwrap_or(json!({}));
            let zero_result = zero_result_from_request(&normalized);

            if let Err(e) = snapshots::upsert_empty_snapshot(
                &state.mongo.db,
                &request_key,
                normalized_json,
                zero_result.clone(),
            )
            .await
            {
                tracing::error!(
                    request_key=%request_key,
                    error=%e,
                    "failed to upsert zero snapshot"
                );
            }

            match refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await {
                Ok(did_queue) => {
                    tracing::info!(
                        request_key=%request_key,
                        did_queue=did_queue,
                        "refresh job enqueue_or_requeue result"
                    );

                    if did_queue {
                        let _ =
                            snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued")
                                .await;
                    }
                }
                Err(e) => tracing::error!(
                    request_key=%request_key,
                    error=%e,
                    "failed to enqueue refresh job"
                ),
            }

            Json(BalanceResponse {
                status: true,
                is_complete: false,
                has_changed: true, // ✅ NEW: Fresh creation is a "change"
                result: zero_result,
                error: None,
            })
            .into_response()
        }

        Err(e) => {
            tracing::error!(
                request_key=%request_key,
                error=%e,
                "snapshot fetch error (fail-soft)"
            );

            let zero_result = zero_result_from_request(&normalized);

            Json(BalanceResponse {
                status: true,
                is_complete: false,
                has_changed: false, // ✅ NEW
                result: zero_result,
                error: None,
            })
            .into_response()
        }
    }
}

/// OPTIONAL: If you ever want to return errors explicitly from inside this handler:
#[allow(dead_code)]
fn bad_request(
    code: &str,
    message: &str,
    details: serde_json::Value,
) -> (StatusCode, Json<BalanceResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(BalanceResponse {
            status: false,
            is_complete: false,
            has_changed: false, // ✅ NEW
            result: json!({}),
            error: Some(crate::http::error::ApiErrorBody {
                code: code.to_string(),
                message: message.to_string(),
                details: Some(details),
            }),
        }),
    )
}