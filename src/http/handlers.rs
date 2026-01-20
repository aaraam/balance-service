// ==================================================
// balance-service\src\http\handlers.rs
// ==================================================

use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::db::{refresh_jobs, snapshots};
use crate::http::dto::{zero_result_from_request, BalanceRequest, BalanceResponse};
use crate::http::validate::{validate_normalized_request, validate_request_limits};
use crate::AppState;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use bson::DateTime;
use serde_json::json;

const STALE_AFTER_SECS: i64 = 30;

/// Hard refresh cooldown (prevents hammering)
const HARD_REFRESH_COOLDOWN_SECS: i64 = 10;

pub async fn health() -> &'static str {
    "ok"
}

// ✅ Lightweight status check for Node.js poller
pub async fn get_job_status(
    State(state): State<AppState>,
    Path(request_key): Path<String>,
) -> impl IntoResponse {
    match snapshots::get_snapshot_status(&state.mongo.db, &request_key).await {
        Ok(Some(status)) => Json(json!({
            "status": true,
            "isComplete": status.is_complete,
            "hasChanged": status.has_changed,
            "requestKey": status.request_key
        }))
        .into_response(),
        Ok(None) => Json(json!({
            "status": false,
            "message": "Job not found",
            "isComplete": false,
            "hasChanged": false
        }))
        .into_response(),
        Err(e) => {
            tracing::error!("Status check error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn get_multi_wallet_balances(
    State(state): State<AppState>,
    Json(req): Json<BalanceRequest>,
) -> impl IntoResponse {
    // 1) Validate raw limits first
    if let Err(e) = validate_request_limits(&req) {
        return e.into_response();
    }

    // 2) Normalize
    let normalized = normalize_request(&req);

    // 3) Validate normalized request
    if let Err(e) = validate_normalized_request(&normalized) {
        return e.into_response();
    }

    let canonical = serde_json::to_string(&normalized).unwrap_or_else(|_| "{}".to_string());
    let request_key = request_key_from_canonical_json(&canonical);

    tracing::info!(
        request_key = %request_key,
        hard_refresh = normalized.hard_refresh,
        "balances request received"
    );

    let now = DateTime::now();

    match snapshots::get_snapshot(&state.mongo.db, &request_key).await {
        Ok(Some(doc)) => {
            let age_secs =
                (now.timestamp_millis() - doc.last_updated_at.timestamp_millis()) / 1000;
            let is_stale = age_secs > STALE_AFTER_SECS;

            // ✅ CRITICAL FIX: Cooldown applies to implicit stale refresh too
            let in_cooldown = age_secs < HARD_REFRESH_COOLDOWN_SECS;
            let should_refresh = (is_stale || normalized.hard_refresh) && !in_cooldown;

            tracing::debug!(
                request_key = %request_key,
                age_secs = age_secs,
                is_stale = is_stale,
                should_refresh = should_refresh,
                "snapshot hit"
            );

            if should_refresh {
                // Try to enqueue. Note: enqueue_or_requeue dedups automatically.
                if let Ok(did_queue) =
                    refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await
                {
                    if did_queue {
                        let _ = snapshots::set_refresh_state(
                            &state.mongo.db,
                            &request_key,
                            "queued",
                        )
                        .await;
                    }
                }
            } else if normalized.hard_refresh && in_cooldown {
                tracing::debug!(
                    request_key = %request_key,
                    "hard_refresh skipped due to cooldown"
                );
            }

            Json(BalanceResponse {
                status: true,
                is_complete: doc.is_complete,
                has_changed: doc.has_changed,
                request_key: request_key.clone(),
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

            // Upsert empty snapshot (hasChanged = false)
            let _ = snapshots::upsert_empty_snapshot(
                &state.mongo.db,
                &request_key,
                normalized_json,
                zero_result.clone(),
            )
            .await;

            if let Ok(did_queue) =
                refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await
            {
                if did_queue {
                    let _ =
                        snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued").await;
                }
            }

            Json(BalanceResponse {
                status: true,
                is_complete: false,
                has_changed: false, // Default false on miss to avoid spam
                request_key: request_key.clone(),
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
                has_changed: false,
                request_key: request_key.clone(),
                result: zero_result,
                error: None,
            })
            .into_response()
        }
    }
}