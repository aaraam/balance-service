use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::db::{refresh_jobs, snapshots};
use crate::http::dto::{empty_legacy_result, BalanceRequest, BalanceResponse};
use crate::AppState;

use axum::{extract::State, Json};
use bson::DateTime;
use serde_json::json;

const STALE_AFTER_SECS: i64 = 30;

pub async fn health() -> &'static str {
    "ok"
}

pub async fn get_multi_wallet_balances(
    State(state): State<AppState>,
    Json(req): Json<BalanceRequest>,
) -> Json<BalanceResponse> {
    let normalized = normalize_request(&req);
    let canonical = serde_json::to_string(&normalized).unwrap_or_else(|_| "{}".to_string());
    let request_key = request_key_from_canonical_json(&canonical);

    tracing::info!(
        request_key = %request_key,
        wallets = ?normalized.walletAddresses,
        contracts = ?normalized.contracts.iter().map(|c| (&c.networkName, c.contractAddresses.len())).collect::<Vec<_>>(),
        "balances request received"
    );

    let now = DateTime::now();

    match snapshots::get_snapshot(&state.mongo.db, &request_key).await {
        Ok(Some(doc)) => {
            let age_secs =
                (now.timestamp_millis() - doc.last_updated_at.timestamp_millis()) / 1000;
            let is_stale = age_secs > STALE_AFTER_SECS;

            tracing::debug!(
                request_key = %request_key,
                age_secs = age_secs,
                is_stale = is_stale,
                refresh_state = %doc.refresh_state,
                "snapshot hit"
            );

            if is_stale {
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
                            tracing::debug!(request_key=%request_key, "snapshot refreshState set to queued");
                        }
                    }
                    Err(e) => {
                        tracing::error!(request_key=%request_key, error=%e, "failed to enqueue refresh job");
                    }
                }
            }

            Json(BalanceResponse {
                status: true,
                result: doc.result,
            })
        }

        Ok(None) => {
            tracing::info!(request_key=%request_key, "snapshot miss → creating empty snapshot + enqueue job");

            let normalized_json = serde_json::to_value(&normalized).unwrap_or(json!({}));
            let empty_result = empty_legacy_result();

            if let Err(e) = snapshots::upsert_empty_snapshot(
                &state.mongo.db,
                &request_key,
                normalized_json,
                empty_result.clone(),
            )
            .await
            {
                tracing::error!(request_key=%request_key, error=%e, "failed to upsert empty snapshot");
            } else {
                tracing::debug!(request_key=%request_key, "empty snapshot upserted");
            }

            match refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await {
                Ok(did_queue) => {
                    tracing::info!(request_key=%request_key, did_queue=did_queue, "refresh job enqueue_or_requeue result");

                    if did_queue {
                        let _ =
                            snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued")
                                .await;
                        tracing::debug!(request_key=%request_key, "snapshot refreshState set to queued");
                    }
                }
                Err(e) => tracing::error!(request_key=%request_key, error=%e, "failed to enqueue refresh job"),
            }

            Json(BalanceResponse {
                status: true,
                result: empty_result,
            })
        }

        Err(e) => {
            tracing::error!(request_key=%request_key, error=%e, "snapshot fetch error (fail-soft)");
            Json(BalanceResponse {
                status: true,
                result: empty_legacy_result(),
            })
        }
    }
}
