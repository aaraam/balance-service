use crate::core::key::request_key_from_canonical_json;
use crate::core::normalize::normalize_request;
use crate::db::{ refresh_jobs, snapshots, sol_refresh_jobs, sol_snapshots };
use crate::http::dto::{ BalanceRequest, BalanceResponse, BalanceResponseMeta };
use crate::AppState;

use axum::{ extract::{ Query, State }, Json };
use bson::DateTime;
use serde::Deserialize;
use serde_json::json;

const STALE_AFTER_SECS: i64 = 30;

// bounded polling defaults
const WAIT_POLL_INTERVAL_MS: u64 = 50;
const WAIT_MAX_MS: u64 = 2000;

#[derive(Debug, Deserialize)]
pub struct BalanceQuery {
    pub wait_ms: Option<u64>,
}

pub async fn health() -> &'static str {
    "ok"
}

fn meta_from_parts(
    request_key: &str,
    now: DateTime,
    evm_last: Option<DateTime>,
    evm_state: Option<&str>,
    sol_last: Option<DateTime>,
    sol_state: Option<&str>
) -> BalanceResponseMeta {
    // overall lastUpdated = max(evm_last, sol_last) where present
    let last = match (evm_last, sol_last) {
        (Some(a), Some(b)) => if a.timestamp_millis() >= b.timestamp_millis() { a } else { b }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => now,
    };

    // overall state: worst of (running > queued > failed > idle)
    fn rank(s: &str) -> i32 {
        match s {
            "running" => 4,
            "queued" => 3,
            "failed" => 2,
            "idle" => 1,
            _ => 0,
        }
    }
    let mut best_state = "idle";
    for s in [evm_state.unwrap_or("idle"), sol_state.unwrap_or("idle")] {
        if rank(s) > rank(best_state) {
            best_state = s;
        }
    }

    let age_secs = (now.timestamp_millis() - last.timestamp_millis()) / 1000;
    BalanceResponseMeta {
        request_key: request_key.to_string(),
        refresh_state: best_state.to_string(),
        last_updated_at_ms: last.timestamp_millis(),
        age_secs,
    }
}

fn merge_results(mut evm: serde_json::Value, sol: serde_json::Value) -> serde_json::Value {
    // merge data rows by walletAddress: union of balance objects
    let evm_data = evm.get_mut("data").and_then(|v| v.as_array_mut());
    let sol_data = sol.get("data").and_then(|v| v.as_array());

    if let (Some(evm_arr), Some(sol_arr)) = (evm_data, sol_data) {
        // build index for evm rows
        let mut idx: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (i, row) in evm_arr.iter().enumerate() {
            if let Some(wa) = row.get("walletAddress").and_then(|x| x.as_str()) {
                idx.insert(wa.to_string(), i);
            }
        }

        for srow in sol_arr {
            let wa = match srow.get("walletAddress").and_then(|x| x.as_str()) {
                Some(x) => x.to_string(),
                None => continue,
            };

            let s_bal = srow.get("balance").and_then(|x| x.as_object()).cloned();
            if s_bal.is_none() {
                continue;
            }
            let s_bal = s_bal.unwrap();

            if let Some(i) = idx.get(&wa).copied() {
                if let Some(ebal) = evm_arr[i].get_mut("balance").and_then(|x| x.as_object_mut()) {
                    for (k, v) in s_bal {
                        ebal.insert(k, v);
                    }
                }
            } else {
                evm_arr.push(srow.clone());
                idx.insert(wa, evm_arr.len() - 1);
            }
        }
    }

    // merge totals.balance objects shallow
    let evm_tot = evm
        .get_mut("total")
        .and_then(|v| v.get_mut("balance"))
        .and_then(|v| v.as_object_mut());

    let sol_tot = sol
        .get("total")
        .and_then(|v| v.get("balance"))
        .and_then(|v| v.as_object())
        .cloned();

    if let (Some(evm_tot), Some(sol_tot)) = (evm_tot, sol_tot) {
        for (k, v) in sol_tot {
            evm_tot.insert(k, v);
        }
    }

    evm
}

async fn wait_for_fresh_snapshot_pair(
    state: &AppState,
    request_key: &str,
    evm_baseline_ms: i64,
    sol_baseline_ms: i64,
    need_evm: bool,
    need_sol: bool,
    wait_ms: u64
) -> Option<(crate::db::models::BalanceSnapshotDoc, crate::db::models::BalanceSnapshotDoc)> {
    let wait_ms = wait_ms.min(WAIT_MAX_MS);
    if wait_ms == 0 {
        return None;
    }

    let start = tokio::time::Instant::now();
    let timeout = std::time::Duration::from_millis(wait_ms);

    loop {
        if start.elapsed() >= timeout {
            return None;
        }

        let evm_doc = snapshots::get_snapshot(&state.mongo.db, request_key).await.ok().flatten();
        let sol_doc = sol_snapshots::get_snapshot(&state.mongo.db, request_key).await.ok().flatten();

        let evm_ok = if need_evm {
            evm_doc
                .as_ref()
                .map(|d| d.refresh_state == "idle" && d.last_updated_at.timestamp_millis() > evm_baseline_ms)
                .unwrap_or(false)
        } else {
            true
        };

        let sol_ok = if need_sol {
            sol_doc
                .as_ref()
                .map(|d| d.refresh_state == "idle" && d.last_updated_at.timestamp_millis() > sol_baseline_ms)
                .unwrap_or(false)
        } else {
            true
        };

        if evm_ok && sol_ok {
            if let (Some(e), Some(s)) = (evm_doc, sol_doc) {
                return Some((e, s));
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(WAIT_POLL_INTERVAL_MS)).await;
    }
}

pub async fn get_multi_wallet_balances(
    State(state): State<AppState>,
    Query(q): Query<BalanceQuery>,
    Json(req): Json<BalanceRequest>
) -> Json<BalanceResponse> {
    let normalized = normalize_request(&req);
    let canonical = serde_json::to_string(&normalized).unwrap_or_else(|_| "{}".to_string());
    let request_key = request_key_from_canonical_json(&canonical);

    let now = DateTime::now();
    let wait_ms = q.wait_ms.unwrap_or(0).min(WAIT_MAX_MS);

    let wants_sol =
        !normalized.solana_wallet_addresses.is_empty()
        || normalized.contracts.iter().any(|c| c.network_name == "sol");

    // ensure both snapshots exist (baseline zeros)
    let normalized_json = serde_json::to_value(&normalized).unwrap_or(json!({}));
    let empty_result = crate::http::dto::zero_legacy_result_from_request(&normalized);

    // --- EVM snapshot ensure ---
    if snapshots::get_snapshot(&state.mongo.db, &request_key).await.ok().flatten().is_none() {
        let _ = snapshots::upsert_empty_snapshot(
            &state.mongo.db,
            &request_key,
            normalized_json.clone(),
            empty_result.clone()
        ).await;
    }

    // --- SOL snapshot ensure ---
    if wants_sol {
        if sol_snapshots::get_snapshot(&state.mongo.db, &request_key).await.ok().flatten().is_none() {
            let _ = sol_snapshots::upsert_empty_snapshot(
                &state.mongo.db,
                &request_key,
                normalized_json.clone(),
                empty_result.clone()
            ).await;
        }
    }

    // fetch both
    let evm_doc = snapshots::get_snapshot(&state.mongo.db, &request_key).await.ok().flatten();
    let sol_doc = if wants_sol {
        sol_snapshots::get_snapshot(&state.mongo.db, &request_key).await.ok().flatten()
    } else {
        None
    };

    // compute staleness per pipeline
    let (evm_is_stale, evm_baseline_ms, evm_state) = if let Some(d) = &evm_doc {
        let age = (now.timestamp_millis() - d.last_updated_at.timestamp_millis()) / 1000;
        (age > STALE_AFTER_SECS, d.last_updated_at.timestamp_millis(), d.refresh_state.clone())
    } else {
        (true, now.timestamp_millis(), "idle".to_string())
    };

    let (sol_is_stale, sol_baseline_ms, sol_state) = if wants_sol {
        if let Some(d) = &sol_doc {
            let age = (now.timestamp_millis() - d.last_updated_at.timestamp_millis()) / 1000;
            (age > STALE_AFTER_SECS, d.last_updated_at.timestamp_millis(), d.refresh_state.clone())
        } else {
            (true, now.timestamp_millis(), "idle".to_string())
        }
    } else {
        (false, now.timestamp_millis(), "idle".to_string())
    };

    // enqueue EVM job if needed
    let mut evm_state_out = evm_state.clone();
    if evm_is_stale || normalized.hard_refresh {
        if let Ok(did_queue) = refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await {
            if did_queue {
                let _ = snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued").await;
                evm_state_out = "queued".to_string();
            }
        }
    }

    // enqueue SOL job if needed (separately)
    let mut sol_state_out = sol_state.clone();
    if wants_sol && (sol_is_stale || normalized.hard_refresh) {
        if let Ok(did_queue) = sol_refresh_jobs::enqueue_or_requeue(&state.mongo.db, &request_key).await {
            if did_queue {
                let _ = sol_snapshots::set_refresh_state(&state.mongo.db, &request_key, "queued").await;
                sol_state_out = "queued".to_string();
            }
        }
    }

    // bounded wait for BOTH pipelines if requested
    if wait_ms > 0 {
        let need_evm = evm_is_stale || normalized.hard_refresh || evm_state_out != "idle";
        let need_sol = wants_sol && (sol_is_stale || normalized.hard_refresh || sol_state_out != "idle");

        if let Some((fresh_evm, fresh_sol)) = wait_for_fresh_snapshot_pair(
            &state,
            &request_key,
            evm_baseline_ms,
            sol_baseline_ms,
            need_evm,
            need_sol,
            wait_ms
        ).await {
            let merged = merge_results(fresh_evm.result.clone(), fresh_sol.result.clone());
            let meta = meta_from_parts(
                &request_key,
                DateTime::now(),
                Some(fresh_evm.last_updated_at),
                Some(&fresh_evm.refresh_state),
                Some(fresh_sol.last_updated_at),
                Some(&fresh_sol.refresh_state)
            );

            return Json(BalanceResponse {
                status: true,
                result: merged,
                meta: Some(meta),
            });
        }
    }

    // DO NOT move evm_doc/sol_doc before using them in meta.
    let evm_result = evm_doc
        .as_ref()
        .map(|d| d.result.clone())
        .unwrap_or_else(|| empty_result.clone());

    let sol_result = sol_doc
        .as_ref()
        .map(|d| d.result.clone())
        .unwrap_or_else(|| empty_result.clone());

    let merged = if wants_sol { merge_results(evm_result, sol_result) } else { evm_result };

    let meta = meta_from_parts(
        &request_key,
        now,
        evm_doc.as_ref().map(|d| d.last_updated_at),
        Some(&evm_state_out),
        sol_doc.as_ref().map(|d| d.last_updated_at),
        if wants_sol { Some(&sol_state_out) } else { None }
    );

    Json(BalanceResponse {
        status: true,
        result: merged,
        meta: Some(meta),
    })
}
