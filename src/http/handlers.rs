use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::db::{refresh_jobs, snapshots};
use crate::http::dto::{zero_result_from_request, BalanceRequest, BalanceResponse};
use crate::http::validate::validate_request_limits;
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
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

    let canonical_json = match serde_json::to_string(&normalized) {
        Ok(s) => s,
        Err(e) => {
            return crate::http::error::ApiError::bad_request(
                "JSON_ERROR",
                &e.to_string(),
                None,
            )
            .into_response();
        }
    };

    let request_key = request_key_from_canonical_json(&canonical_json);
    let zero_result = zero_result_from_request(&normalized);

    let snap = snapshots::get_snapshot(&state.mongo.db, &request_key).await;

    match snap {
        Ok(Some(existing)) => {
            let existing_is_complete = existing.is_complete;
            let should_refresh = !existing_is_complete;

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
                        snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued")
                            .await;
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