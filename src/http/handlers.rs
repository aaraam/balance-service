use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::db::{refresh_jobs, snapshots};
use crate::http::dto::{zero_result_from_request, BalanceRequest, BalanceResponse};
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
        hard_refresh = normalized.hard_refresh,
        evm_wallets = normalized.wallet_addresses.len(),
        sol_wallets = normalized.solana_wallet_addresses.len(),
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
                "snapshot hit"
            );

            if is_stale || normalized.hard_refresh {
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
            }

            Json(BalanceResponse {
                status: true,
                result: doc.result,
            })
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
                result: zero_result,
            })
        }

        Err(e) => {
            tracing::error!(
                request_key=%request_key,
                error=%e,
                "snapshot fetch error (fail-soft)"
            );

            // IMPORTANT: still return standard-shaped zeros
            let zero_result = zero_result_from_request(&normalized);

            Json(BalanceResponse {
                status: true,
                result: zero_result,
            })
        }
    }
}
