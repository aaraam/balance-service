use crate::AppState;
use anyhow::anyhow;
use bson::{doc, DateTime};
use futures::stream::{self, StreamExt};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use serde_json::json;
use tokio::time::Duration;

use crate::chains::{is_ignored_network, supported_evm_networks};
use crate::core::chains_meta::native_symbol_for;
use crate::core::normalize::normalize_request;
use crate::evm::format::u256_to_decimal_string;
use crate::evm::multicall3::{
    fetch_balances_multicall3, fetch_token_decimals_multicall3, EvmBalances,
};
use crate::evm::rpc::RpcClient;
use crate::http::dto::{trc20_contracts_from_request, zero_result_from_request, BalanceRequest};
use crate::solana::rpc::SolanaRpcClient;
use crate::tron::multicall3::{
    fetch_all_tron_balances, fetch_native_trx_concurrent, fetch_trc20_decimals,
    MAX_TRON_CALLS_PER_BATCH, TRON_NATIVE_CONCURRENCY,
};
use crate::tron::rpc::TronRpcClient;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const MAX_CALLS_PER_BATCH: usize = 600;
const NATIVE_DECIMALS: u32 = 18;
const SOL_DECIMALS: u32 = 9;

// TRON phase guards — prevent large requests from turning into latency grenades.
const TRON_MAX_WALLETS_PER_JOB: usize = 100;
const TRON_MAX_TOKENS_PER_JOB: usize = 20;
const TRON_MAX_PAIR_CALLS_PER_JOB: usize = 1000;

/// Hard wall-clock budget for the entire TRON phase.
/// If TRON exceeds this,
/// EVM/SOL results are already persisted and the job completes without TRON.
// Must be larger than (TRON_CALL_TIMEOUT_MS/1000 * worst-case sequential depth).
// Native calls run concurrently, but TRC20 pair chunks run sequentially.
// Budget = (chunks * per_call_timeout) + headroom.
// 20s is safe for typical request sizes.
const TRON_TOTAL_PHASE_TIMEOUT_SECS: u64 = 25;

// Per-call HTTP timeout for TRON calls — deliberately shorter than EVM rpc_timeout_ms.
// Keeps a single slow call from eating the entire phase budget.
const TRON_CALL_TIMEOUT_MS: u64 = 5_000;

// --- metrics ---
static JOBS_CLAIMED: AtomicU64 = AtomicU64::new(0);
static JOBS_DONE: AtomicU64 = AtomicU64::new(0);
static JOBS_FAILED: AtomicU64 = AtomicU64::new(0);

static EVM_NET_OK: AtomicU64 = AtomicU64::new(0);
static EVM_NET_FAIL: AtomicU64 = AtomicU64::new(0);

static SOL_NET_OK: AtomicU64 = AtomicU64::new(0);
static SOL_NET_FAIL: AtomicU64 = AtomicU64::new(0);

static TRON_NET_OK: AtomicU64 = AtomicU64::new(0);
static TRON_NET_FAIL: AtomicU64 = AtomicU64::new(0);

fn is_valid_solana_pubkey_32(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
    let decoded = match bs58::decode(t).into_vec() {
        Ok(v) => v,
        Err(_) => return false,
    };
    decoded.len() == 32
}

fn u128_base_units_to_fixed_18(value: u128, decimals: u32) -> String {
    if decimals == 0 {
        return format!("{}.{}", value, "0".repeat(18));
    }

    let out_decimals: u32 = 18;
    let pow = (10u128).checked_pow(decimals.min(38)).unwrap_or(u128::MAX);
    if pow == u128::MAX && decimals > 38 {
        return format!("0.{}", "0".repeat(18));
    }

    let whole = if pow == 0 { 0 } else { value / pow };
    let frac = if pow == 0 { 0 } else { value % pow };

    let mut frac_str = frac.to_string();
    let dec_usize = decimals as usize;

    if frac_str.len() < dec_usize {
        frac_str = format!("{}{}", "0".repeat(dec_usize - frac_str.len()), frac_str);
    }

    if out_decimals > decimals {
        frac_str.push_str(&"0".repeat((out_decimals - decimals) as usize));
    } else if out_decimals < decimals {
        frac_str.truncate(out_decimals as usize);
    }

    format!("{}.{}", whole, frac_str)
}

fn u256_base_units_to_fixed_18(value: ethereum_types::U256, decimals: u32) -> String {
    if decimals == 0 {
        return format!("{}.{}", value, "0".repeat(18));
    }

    let decimal = u256_to_decimal_string(value, decimals, false);
    let (whole, fraction) = decimal.split_once('.').unwrap_or((&decimal, ""));
    let mut fraction = fraction.to_string();

    if fraction.len() < 18 {
        fraction.push_str(&"0".repeat(18 - fraction.len()));
    } else if fraction.len() > 18 {
        fraction.truncate(18);
    }

    format!("{}.{}", whole, fraction)
}

fn lamports_u128_to_sol_fixed_18(lamports: u128) -> String {
    u128_base_units_to_fixed_18(lamports, SOL_DECIMALS)
}

fn sun_to_trx_fixed_18(sun: u64) -> String {
    u128_base_units_to_fixed_18(sun as u128, 6)
}

fn sol_mints_from_request(req: &BalanceRequest) -> Vec<String> {
    req.contracts
        .iter()
        .find(|c| c.network_name == "sol")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default()
}

/// Persist the current in-memory `final_result` into the snapshot document.
/// Called after each chain-family phase so the API can return partial data
/// without waiting for the full job to complete.
async fn persist_partial_result(
    snapshots: &mongodb::Collection<bson::Document>,
    request_key: &str,
    final_result: &serde_json::Value,
    req_sanitized_json: &serde_json::Value,
    stage: &str,
) -> Result<(), anyhow::Error> {
    snapshots
        .update_one(
            doc! { "requestKey": request_key },
            doc! {
                "$set": {
                    "lastUpdatedAt": DateTime::now(),
                    "refreshState": "running",
                    "isComplete": false,
                    "progressStage": stage,
                    "result": bson::to_bson(final_result).unwrap_or(bson::Bson::Null),
                    "normalizedRequest": bson::to_bson(req_sanitized_json).unwrap_or(bson::Bson::Null),
                }
            },
        )
        .await?;
    Ok(())
}

pub async fn run_worker(state: AppState) {
    let worker_pool = state.cfg.worker_concurrency.max(1) as usize;

    tracing::info!(
        worker_enabled = state.cfg.worker_enabled,
        worker_pool = worker_pool,
        "worker started"
    );

    if state.cfg.nats_url.trim().is_empty() {
        tracing::warn!("NATS_URL is empty; falling back to Mongo scan loop");
        run_worker_mongo_fallback(state).await;
        return;
    }

    let queue = state.queue.clone();
    if let Err(e) = queue.ensure_stream().await {
        tracing::error!(error = %e, "failed to ensure JetStream stream; falling back to Mongo scan loop");
        run_worker_mongo_fallback(state).await;
        return;
    }

    let consumer = match queue.get_or_create_consumer().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to get/create JetStream consumer; falling back to Mongo scan loop");
            run_worker_mongo_fallback(state).await;
            return;
        }
    };

    loop {
        if !state.cfg.worker_enabled {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            continue;
        }

        let poll_ms = state.cfg.worker_poll_ms.max(50);

        let mut batch_stream = match consumer.fetch().max_messages(32).messages().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "JetStream fetch failed; sleeping 1000ms");
                tokio::time::sleep(Duration::from_millis(1000)).await;
                continue;
            }
        };

        let mut fetched = Vec::new();
        while let Some(msg_result) = batch_stream.next().await {
            if let Ok(msg) = msg_result {
                fetched.push(msg);
            }
        }

        if fetched.is_empty() {
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            continue;
        }

        let state_ref = &state;

        stream::iter(fetched)
            .for_each_concurrent(worker_pool, |msg| async move {
                let request_key = match std::str::from_utf8(msg.payload.as_ref()) {
                    Ok(k) => k.trim().to_string(),
                    Err(e) => {
                        tracing::warn!(error = %e, "dropping message: cannot decode requestKey");
                        let _ = msg.ack().await;
                        return;
                    }
                };

                async fn claim_job_by_key(state: &AppState, request_key: &str) -> Result<bool, mongodb::error::Error> {
                    let coll = state.mongo.db.collection::<bson::Document>("balance_refresh_jobs");
                    let now = DateTime::now();
                    let filter = doc! {
                        "requestKey": request_key,
                        "status": "queued",
                        "$or": [
                            { "nextRetryAt": bson::Bson::Null },
                            { "nextRetryAt": { "$lte": now } }
                        ]
                    };

                    let update = doc! { "$set": { "status": "running", "updatedAt": now } };
                    let opts = FindOneAndUpdateOptions::builder().return_document(ReturnDocument::After).build();

                    let doc_opt = coll.find_one_and_update(filter, update).with_options(opts).await?;
                    Ok(doc_opt.is_some())
                }

                match claim_job_by_key(state_ref, &request_key).await {
                    Ok(true) => {
                        JOBS_CLAIMED.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(request_key = %request_key, "claimed job");

                        let job_start = Instant::now();
                        let res = process_job(state_ref, &request_key).await;

                        match res {
                            Ok(_) => {
                                JOBS_DONE.fetch_add(1, Ordering::Relaxed);
                                tracing::info!(
                                    request_key = %request_key,
                                    elapsed_ms = job_start.elapsed().as_millis(),
                                    "job done"
                                );
                                let _ = msg.ack().await;
                            }
                            Err(e) => {
                                JOBS_FAILED.fetch_add(1, Ordering::Relaxed);
                                tracing::error!(
                                    request_key = %request_key,
                                    elapsed_ms = job_start.elapsed().as_millis(),
                                    error = %e,
                                    "job failed"
                                );

                                let _ = mark_job_failed(state_ref, &request_key).await;
                                let _ = msg.ack_with(async_nats::jetstream::AckKind::Nak(None)).await;
                            }
                        }
                    }
                    Ok(false) => {
                        let _ = msg.ack().await;
                    }
                    Err(e) => {
                        tracing::error!(request_key=%request_key, error=%e, "failed to claim job in Mongo");
                        let _ = msg.ack_with(async_nats::jetstream::AckKind::Nak(None)).await;
                    }
                }
            })
            .await;
    }
}

async fn run_worker_mongo_fallback(state: AppState) {
    let poll_ms = state.cfg.worker_poll_ms;

    tracing::info!(
        worker_enabled = state.cfg.worker_enabled,
        poll_ms = poll_ms,
        "worker mongo fallback started"
    );

    loop {
        if !state.cfg.worker_enabled {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            continue;
        }

        match claim_next_job(&state).await {
            Ok(Some(request_key)) => {
                JOBS_CLAIMED.fetch_add(1, Ordering::Relaxed);
                tracing::info!(request_key = %request_key, "claimed job (mongo)");

                let job_start = Instant::now();
                let res = process_job(&state, &request_key).await;

                match res {
                    Ok(_) => {
                        JOBS_DONE.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(
                            request_key = %request_key,
                            elapsed_ms = job_start.elapsed().as_millis(),
                            "job done (mongo)"
                        );
                    }
                    Err(e) => {
                        JOBS_FAILED.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(
                            request_key = %request_key,
                            elapsed_ms = job_start.elapsed().as_millis(),
                            error = %e,
                            "job failed (mongo)"
                        );
                        let _ = mark_job_failed(&state, &request_key).await;
                    }
                }
            }
            Ok(None) => {
                tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
            }
            Err(e) => {
                tracing::error!(error = %e, "worker claim error -> sleeping 1000ms");
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            }
        }
    }
}

async fn claim_next_job(state: &AppState) -> Result<Option<String>, mongodb::error::Error> {
    let coll = state.mongo.db.collection::<bson::Document>("balance_refresh_jobs");
    let now = DateTime::now();

    let filter = doc! {
        "status": "queued",
        "$or": [
            { "nextRetryAt": bson::Bson::Null },
            { "nextRetryAt": { "$lte": now } }
        ]
    };

    let update = doc! {
        "$set": { "status": "running", "updatedAt": now }
    };

    let opts = FindOneAndUpdateOptions::builder()
        .sort(doc! { "createdAt": 1 })
        .return_document(ReturnDocument::After)
        .build();

    let doc_opt = coll.find_one_and_update(filter, update).with_options(opts).await?;
    Ok(doc_opt.and_then(|d| d.get_str("requestKey").ok().map(|s| s.to_string())))
}

async fn process_job(state: &AppState, request_key: &str) -> Result<(), anyhow::Error> {
    let snapshots = state.mongo.db.collection::<bson::Document>("balance_snapshots");
    let now = DateTime::now();

    snapshots.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "refreshState": "running", "isComplete": false } },
    ).await?;

    let snap = snapshots.find_one(doc! { "requestKey": request_key }).await?
        .ok_or_else(|| anyhow!("snapshot not found for requestKey"))?;

    let initial_result_json: serde_json::Value = snap
        .get("result")
        .cloned()
        .and_then(|b| bson::from_bson(b).ok())
        .unwrap_or(json!({}));

    let normalized_req_bson = snap
        .get("normalizedRequest")
        .cloned()
        .unwrap_or(bson::Bson::Null);

    let normalized_req_json: serde_json::Value =
        bson::from_bson(normalized_req_bson).unwrap_or_else(|_| json!({}));

    let req_raw: BalanceRequest = serde_json::from_value(normalized_req_json.clone())
        .unwrap_or_else(|_| BalanceRequest {
            hard_refresh: false,
            contracts: vec![],
            wallet_addresses: vec![],
            solana_wallet_addresses: vec![],
            tron_wallet_addresses: vec![],
            doge_wallet_addresses: vec![],
            btc_wallet_addresses: vec![],
        });

    let req: BalanceRequest = normalize_request(&req_raw);
    let req_sanitized_json = serde_json::to_value(&req).unwrap_or_else(|_| json!({}));
    let mut final_result = zero_result_from_request(&req);

    if let Some(existing_bson) = snap.get("result") {
        if existing_bson != &bson::Bson::Null {
            if let Ok(existing_json) = bson::from_bson::<serde_json::Value>(existing_bson.clone()) {
                final_result = existing_json;
            }
        }
    }

    let evm_wallet_index: std::collections::HashMap<String, usize> = req
        .wallet_addresses
        .iter()
        .enumerate()
        .map(|(i, w)| (w.clone(), i))
        .collect();

    let sol_offset = req.wallet_addresses.len();
    let sol_wallet_index: std::collections::HashMap<String, usize> = req
        .solana_wallet_addresses
        .iter()
        .enumerate()
        .map(|(i, w)| (w.clone(), sol_offset + i))
        .collect();

    // ==========================
    // EVM processing
    // ==========================
    let evm_map = supported_evm_networks();

    for cg in &req.contracts {
        let net = cg.network_name.as_str();

        if net == "sol" || net == "trx" {
            continue;
        }

        if is_ignored_network(net) {
            tracing::warn!(network = %net, "ignored network in contracts list");
            continue;
        }

        let Some(chain) = evm_map.get(net).copied() else {
            tracing::warn!(network = %net, "unsupported network in contracts list (ignored)");
            continue;
        };

        let net_start = Instant::now();

        let rpc_url = chain.thirdweb_rpc_url(&state.cfg.thirdweb_client_id);
        let rpc = RpcClient::new(rpc_url, state.cfg.rpc_timeout_ms);

        let balances: EvmBalances = match fetch_balances_multicall3(
            &rpc,
            &req.wallet_addresses,
            &cg.contract_addresses,
            MAX_CALLS_PER_BATCH,
        )
        .await
        {
            Ok(b) => b,
            Err(e) => {
                EVM_NET_FAIL.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    network = %net,
                    error = %e,
                    elapsed_ms = net_start.elapsed().as_millis(),
                    evm_ok = EVM_NET_OK.load(Ordering::Relaxed),
                    evm_fail = EVM_NET_FAIL.load(Ordering::Relaxed),
                    "evm fetch failed -> keeping previous val or zeros"
                );
                continue;
            }
        };

        let decimals_map = match fetch_token_decimals_multicall3(
            &rpc,
            &cg.contract_addresses,
            MAX_CALLS_PER_BATCH,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(network = %net, error = %e, "decimals fetch failed -> default 18");
                std::collections::HashMap::new()
            }
        };

        for w in &req.wallet_addresses {
            let row_idx = *evm_wallet_index
                .get(w)
                .ok_or_else(|| anyhow!("evm wallet index missing"))?;

            let data_arr = final_result
                .get_mut("data")
                .and_then(|v| v.as_array_mut())
                .ok_or_else(|| anyhow!("final_result.data missing or not array"))?;

            let row = data_arr
                .get_mut(row_idx)
                .ok_or_else(|| anyhow!("wallet row missing"))?;

            let bal_obj = row
                .get_mut("balance")
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| anyhow!("balance field not an object"))?;

            if !bal_obj.contains_key(net) {
                bal_obj.insert(net.to_string(), json!({}));
            }
            let chain_obj = bal_obj
                .get_mut(net)
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| anyhow!("balance.{net} missing or not object"))?;

            let native_wei = balances.native.get(w).cloned().unwrap_or_default();
            let native_str = u256_to_decimal_string(native_wei, NATIVE_DECIMALS, false);
            chain_obj.insert(native_symbol_for(net).to_string(), json!(native_str));

            for token_addr in &cg.contract_addresses {
                let raw_bal = balances
                    .erc20
                    .get(w)
                    .and_then(|m| m.get(token_addr))
                    .cloned()
                    .unwrap_or_default();

                let dec = decimals_map.get(token_addr).cloned().unwrap_or(18);
                let s = u256_to_decimal_string(raw_bal, dec, false);

                chain_obj.insert(token_addr.clone(), json!(s));
            }
        }

        let mut native_sum = ethereum_types::U256::zero();

        for w in &req.wallet_addresses {
            native_sum += balances.native.get(w).cloned().unwrap_or_default();
        }

        let totals_balance_obj = final_result
            .get_mut("total")
            .and_then(|v| v.as_object_mut())
            .and_then(|m| m.get_mut("balance"))
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| anyhow!("final_result.total.balance missing or not object"))?;

        if !totals_balance_obj.contains_key(net) {
            totals_balance_obj.insert(net.to_string(), json!({}));
        }

        let total_chain_obj = totals_balance_obj
            .get_mut(net)
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| anyhow!("total.balance.{net} missing or not object"))?;

        total_chain_obj.insert(
            native_symbol_for(net).to_string(),
            json!(u256_to_decimal_string(native_sum, NATIVE_DECIMALS, false)),
        );

        for token_addr in &cg.contract_addresses {
            let mut sum = ethereum_types::U256::zero();

            for w in &req.wallet_addresses {
                let v = balances
                    .erc20
                    .get(w)
                    .and_then(|m| m.get(token_addr))
                    .cloned()
                    .unwrap_or_default();
                sum += v;
            }

            let dec = decimals_map.get(token_addr).cloned().unwrap_or(18);
            total_chain_obj.insert(
                token_addr.clone(),
                json!(u256_to_decimal_string(sum, dec, false)),
            );
        }

        EVM_NET_OK.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            network = %net,
            elapsed_ms = net_start.elapsed().as_millis(),
            evm_ok = EVM_NET_OK.load(Ordering::Relaxed),
            evm_fail = EVM_NET_FAIL.load(Ordering::Relaxed),
            "evm network processed"
        );

        if state.cfg.worker_slow_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(state.cfg.worker_slow_ms)).await;
        }
    }

    // Persist EVM results immediately — SOL and TRON do not block clients from
    // seeing EVM balances anymore.
    persist_partial_result(&snapshots, request_key, &final_result, &req_sanitized_json, "evm_done")
        .await
        .unwrap_or_else(|e| tracing::warn!(error = %e, "partial persist after EVM failed (non-fatal)"));

    // ==========================
    // SOL processing
    // ==========================
    if !req.solana_wallet_addresses.is_empty() {
        let sol_start = Instant::now();

        let sol_rpc = SolanaRpcClient::new(state.cfg.solana_rpc_url.clone(), state.cfg.rpc_timeout_ms);
        let sol_mints = sol_mints_from_request(&req);

        let mut sol_total_lamports: u128 = 0;
        let mut spl_totals: std::collections::HashMap<String, u128> = std::collections::HashMap::new();
        let mut spl_decimals: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

        for w in &req.solana_wallet_addresses {
            let row_idx = *sol_wallet_index
                .get(w)
                .ok_or_else(|| anyhow!("sol wallet index missing"))?;

            let lamports = sol_rpc.get_balance_lamports(w).await.unwrap_or_else(|e| {
                SOL_NET_FAIL.fetch_add(1, Ordering::Relaxed);
                tracing::error!(wallet = %w, error = %e, "sol getBalance failed -> keeping zero");
                0u64
            });

            sol_total_lamports = sol_total_lamports.saturating_add(lamports as u128);

            {
                let data_arr = final_result
                    .get_mut("data")
                    .and_then(|v| v.as_array_mut())
                    .ok_or_else(|| anyhow!("final_result.data missing or not array"))?;

                let row = data_arr
                    .get_mut(row_idx)
                    .ok_or_else(|| anyhow!("wallet row missing"))?;

                let bal_obj = row
                    .get_mut("balance")
                    .and_then(|v| v.as_object_mut())
                    .ok_or_else(|| anyhow!("balance field not an object"))?;

                if !bal_obj.contains_key("sol") {
                    bal_obj.insert("sol".to_string(), json!({}));
                }
                let sol_obj = bal_obj
                    .get_mut("sol")
                    .and_then(|v| v.as_object_mut())
                    .ok_or_else(|| anyhow!("balance.sol missing or not object"))?;

                sol_obj.insert(
                    "sol".to_string(),
                    json!(lamports_u128_to_sol_fixed_18(lamports as u128)),
                );
            }

            for mint in &sol_mints {
                if !is_valid_solana_pubkey_32(mint) {
                    continue;
                }

                let (amt_u128, dec_u32) = sol_rpc
                    .get_spl_balance_by_owner_mint(w, mint)
                    .await
                    .unwrap_or_else(|e| {
                        SOL_NET_FAIL.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(wallet = %w, mint = %mint, error = %e, "sol spl fetch failed");
                        (0u128, 0u32)
                    });

                if dec_u32 > 0 {
                    spl_decimals.entry(mint.clone()).or_insert(dec_u32);
                }

                *spl_totals.entry(mint.clone()).or_insert(0u128) += amt_u128;

                let final_decimals = if dec_u32 > 0 {
                    dec_u32
                } else {
                    *spl_decimals.get(mint).unwrap_or(&0)
                };

                let formatted = u128_base_units_to_fixed_18(amt_u128, final_decimals);

                let data_arr = final_result
                    .get_mut("data")
                    .and_then(|v| v.as_array_mut())
                    .ok_or_else(|| anyhow!("final_result.data missing or not array"))?;

                let row = data_arr
                    .get_mut(row_idx)
                    .ok_or_else(|| anyhow!("wallet row missing"))?;

                let bal_obj = row
                    .get_mut("balance")
                    .and_then(|v| v.as_object_mut())
                    .ok_or_else(|| anyhow!("balance field not an object"))?;

                if !bal_obj.contains_key("sol") {
                    bal_obj.insert("sol".to_string(), json!({}));
                }
                let sol_obj = bal_obj
                    .get_mut("sol")
                    .and_then(|v| v.as_object_mut())
                    .ok_or_else(|| anyhow!("balance.sol missing or not object"))?;

                sol_obj.insert(mint.clone(), json!(formatted));
            }
        }

        {
            let totals_balance_obj = final_result
                .get_mut("total")
                .and_then(|v| v.as_object_mut())
                .and_then(|m| m.get_mut("balance"))
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| anyhow!("final_result.total.balance missing or not object"))?;

            if !totals_balance_obj.contains_key("sol") {
                totals_balance_obj.insert("sol".to_string(), json!({}));
            }
            if let Some(sol_total_obj) = totals_balance_obj
                .get_mut("sol")
                .and_then(|v| v.as_object_mut())
            {
                sol_total_obj.insert(
                    "sol".to_string(),
                    json!(lamports_u128_to_sol_fixed_18(sol_total_lamports)),
                );

                for mint in &sol_mints {
                    let sum = spl_totals.get(mint).cloned().unwrap_or(0u128);
                    let dec = spl_decimals.get(mint).cloned().unwrap_or(0u32);
                    sol_total_obj
                        .insert(mint.clone(), json!(u128_base_units_to_fixed_18(sum, dec)));
                }
            }
        }

        SOL_NET_OK.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            elapsed_ms = sol_start.elapsed().as_millis(),
            sol_ok = SOL_NET_OK.load(Ordering::Relaxed),
            sol_fail = SOL_NET_FAIL.load(Ordering::Relaxed),
            mints = sol_mints.len(),
            wallets = req.solana_wallet_addresses.len(),
            "sol network processed"
        );
    }

    // Persist EVM + SOL so the client sees these immediately, before TRON runs.
    persist_partial_result(&snapshots, request_key, &final_result, &req_sanitized_json, "sol_done")
        .await
        .unwrap_or_else(|e| tracing::warn!(error = %e, "partial persist after SOL failed (non-fatal)"));

    // ==========================
    // TRON processing (Derived from EVM)
    // ==========================
    let trc20_contracts = trc20_contracts_from_request(&req);
    let has_trx = !trc20_contracts.is_empty() || req.contracts.iter().any(|c| c.network_name == "trx");

    if has_trx && !req.wallet_addresses.is_empty() {
        let tron_rpc = TronRpcClient::new(
            state.cfg.tron_fullnode_url.clone(),
            state.cfg.tron_solidity_url.clone(),
            state.cfg.tron_api_key.clone(),
            TRON_CALL_TIMEOUT_MS, // intentionally shorter than EVM rpc_timeout_ms
        );

        // Build (row_idx, b58_wallet) pairs from EVM wallets
        let mut all_tron_targets: Vec<(usize, String)> = Vec::new();

        for (evm_w, &row_idx) in &evm_wallet_index {
            match crate::tron::address::evm_hex_to_tron_b58(evm_w) {
                Ok(b58) => all_tron_targets.push((row_idx, b58)),
                Err(_) => tracing::warn!(wallet = %evm_w, "failed to derive TRON b58 from EVM"),
            }
        }

        // Apply wallet budget — cap before fan-out
        let tron_targets: Vec<(usize, String)> = all_tron_targets
            .into_iter()
            .take(TRON_MAX_WALLETS_PER_JOB)
            .collect();

        let b58_wallets: Vec<String> = tron_targets.iter().map(|(_, b)| b.clone()).collect();

        // Apply TRC20 budget — decide whether to do full fetch or native-only
        let effective_contracts: Vec<String> = trc20_contracts
            .iter()
            .take(TRON_MAX_TOKENS_PER_JOB)
            .cloned()
            .collect();

        let pair_count = b58_wallets.len() * effective_contracts.len();
        let do_trc20 = !effective_contracts.is_empty() && pair_count <= TRON_MAX_PAIR_CALLS_PER_JOB;

        if !effective_contracts.is_empty() && !do_trc20 {
            tracing::warn!(
                wallets = b58_wallets.len(),
                contracts = trc20_contracts.len(),
                pair_count = pair_count,
                limit = TRON_MAX_PAIR_CALLS_PER_JOB,
                "TRON TRC20 skipped: request too large, fetching native TRX only"
            );
        }

        // Wrap the entire TRON phase in a hard timeout so a slow provider
        // never holds EVM/SOL results hostage.
        let tron_start = Instant::now();
        let tron_phase = async {
            let native_balances = fetch_native_trx_concurrent(
                &tron_rpc,
                &b58_wallets,
                TRON_NATIVE_CONCURRENCY,
            )
            .await;

            let trc20_balances: HashMap<String, HashMap<String, ethereum_types::U256>> =
                if do_trc20 {
                    fetch_all_tron_balances(
                        &tron_rpc,
                        &b58_wallets,
                        &effective_contracts,
                        MAX_TRON_CALLS_PER_BATCH,
                    )
                    .await
                } else {
                    HashMap::new()
                };

            let decimals_map: HashMap<String, u32> =
                if do_trc20 && !b58_wallets.is_empty() {
                    fetch_trc20_decimals(
                        &tron_rpc,
                        &effective_contracts,
                        &b58_wallets[0],
                        MAX_TRON_CALLS_PER_BATCH,
                    )
                    .await
                } else {
                    HashMap::new()
                };

            (native_balances, trc20_balances, decimals_map)
        };

        match tokio::time::timeout(
            Duration::from_secs(TRON_TOTAL_PHASE_TIMEOUT_SECS),
            tron_phase,
        )
        .await
        {
            Ok((native_balances, trc20_balances, decimals_map)) => {
                let mut trx_total_sun: u64 = 0;
                let mut trc20_totals: HashMap<String, ethereum_types::U256> = HashMap::new();

                for (row_idx, b58_w) in &tron_targets {
                    let sun = *native_balances.get(b58_w).unwrap_or(&0);
                    trx_total_sun = trx_total_sun.saturating_add(sun);

                    let data_arr = final_result
                        .get_mut("data")
                        .and_then(|v| v.as_array_mut())
                        .unwrap();

                    let row = data_arr.get_mut(*row_idx).unwrap();
                    let bal_obj = row.get_mut("balance").and_then(|v| v.as_object_mut()).unwrap();

                    let trx_obj = bal_obj
                        .entry("trx".to_string())
                        .or_insert_with(|| json!({}))
                        .as_object_mut()
                        .unwrap();

                    trx_obj.insert("trx".to_string(), json!(sun_to_trx_fixed_18(sun)));

                    if let Some(tokens) = trc20_balances.get(b58_w) {
                        for (contract, amount) in tokens {
                            let Some(&dec) = decimals_map.get(contract) else {
                                continue;
                            };
                            let formatted = u256_base_units_to_fixed_18(*amount, dec);
                            trx_obj.insert(contract.clone(), json!(formatted));
                            *trc20_totals
                                .entry(contract.clone())
                                .or_insert_with(ethereum_types::U256::zero) += *amount;
                        }
                    }
                }

                // TRON totals
                let totals_balance_obj = final_result
                    .get_mut("total")
                    .and_then(|v| v.as_object_mut())
                    .and_then(|m| m.get_mut("balance"))
                    .and_then(|v| v.as_object_mut())
                    .unwrap();

                let trx_total_obj = totals_balance_obj
                    .entry("trx".to_string())
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .unwrap();

                trx_total_obj.insert("trx".to_string(), json!(sun_to_trx_fixed_18(trx_total_sun)));

                for (contract, amount) in &trc20_totals {
                    let Some(&dec) = decimals_map.get(contract) else {
                        continue;
                    };
                    trx_total_obj.insert(
                        contract.clone(),
                        json!(u256_base_units_to_fixed_18(*amount, dec)),
                    );
                }

                TRON_NET_OK.fetch_add(1, Ordering::Relaxed);

                tracing::debug!(
                    elapsed_ms = tron_start.elapsed().as_millis(),
                    derived_wallets = b58_wallets.len(),
                    trc20_contracts = effective_contracts.len(),
                    "tron network processed"
                );
            }
            Err(_) => {
                // TRON timed out — EVM/SOL are already persisted. Log and continue.
                TRON_NET_FAIL.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    elapsed_ms = tron_start.elapsed().as_millis(),
                    budget_secs = TRON_TOTAL_PHASE_TIMEOUT_SECS,
                    "TRON phase timed out -> job will complete with EVM/SOL only"
                );
            }
        }
    }

    let has_changed = final_result != initial_result_json;

    snapshots.update_one(
        doc! { "requestKey": request_key },
        doc! {
            "$set": {
                "lastUpdatedAt": now,
                "refreshState": "idle",
                "isComplete": true,
                "hasChanged": has_changed,
                "progressStage": "complete",
                "result": bson::to_bson(&final_result).unwrap_or(bson::Bson::Null),
                "normalizedRequest": bson::to_bson(&req_sanitized_json).unwrap_or(bson::Bson::Null)
            }
        }
    ).await?;

    let jobs = state.mongo.db.collection::<bson::Document>("balance_refresh_jobs");
    jobs.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "status": "done", "updatedAt": now } },
    ).await?;

    Ok(())
}

async fn mark_job_failed(state: &AppState, request_key: &str) -> Result<(), mongodb::error::Error> {
    let jobs = state.mongo.db.collection::<bson::Document>("balance_refresh_jobs");
    let now = DateTime::now();
    let job = jobs.find_one(doc! { "requestKey": request_key }).await?;

    let attempts = job
        .as_ref()
        .and_then(|d| d.get_i32("attempts").ok())
        .unwrap_or(0) + 1;

    let backoff_secs = (attempts as i64) * 5;
    let next_retry_ms = now.timestamp_millis() + backoff_secs * 1000;
    let next_retry = DateTime::from_millis(next_retry_ms);

    jobs.update_one(
        doc! { "requestKey": request_key },
        doc! {
            "$set": {
                "status": "queued",
                "updatedAt": now,
                "nextRetryAt": next_retry
            },
            "$setOnInsert": { "createdAt": now },
            "$inc": { "attempts": 1 }
        },
    ).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethereum_types::U256;

    #[test]
    fn tron_token_formatter_keeps_fixed_18_precision() {
        assert_eq!(
            u256_base_units_to_fixed_18(U256::from(1_500_000u64), 6),
            "1.500000000000000000"
        );
        assert_eq!(
            u256_base_units_to_fixed_18(U256::from(1u64), 18),
            "0.000000000000000001"
        );
    }
}
