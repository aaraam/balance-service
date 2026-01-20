// ==================================================
// FILE: D:\Learn\rust\balance-service\src\worker\runner.rs
// ==================================================

use crate::AppState;
use anyhow::anyhow;
use bson::{ doc, DateTime };
use futures::stream::{ self, StreamExt }; 
use mongodb::options::{ FindOneAndUpdateOptions, ReturnDocument };
use serde_json::json;

use crate::chains::{ is_ignored_network, supported_evm_networks };
use crate::core::normalize::normalize_request;
use crate::evm::format::u256_to_decimal_string;
use crate::evm::multicall3::{
    fetch_balances_multicall3,
    fetch_token_decimals_multicall3,
    EvmBalances,
};
use crate::evm::rpc::RpcClient;
use crate::http::dto::{ zero_result_from_request, BalanceRequest };
use crate::solana::rpc::SolanaRpcClient;
use crate::tron::rpc::TronRpcClient;

use std::sync::atomic::{ AtomicU64, Ordering };
use std::time::Instant;

const MAX_CALLS_PER_BATCH: usize = 600;

/// Limits concurrent HTTP requests to Tron to avoid hitting API rate limits.
const TRON_CONCURRENCY_LIMIT: usize = 5;

const NATIVE_DECIMALS: u32 = 18;
const SOL_DECIMALS: u32 = 9;
const TRX_DECIMALS: u32 = 6;

// --- ultra-light metrics (process-local) ---
static JOBS_CLAIMED: AtomicU64 = AtomicU64::new(0);
static JOBS_DONE: AtomicU64 = AtomicU64::new(0);
static JOBS_FAILED: AtomicU64 = AtomicU64::new(0);

static EVM_NET_OK: AtomicU64 = AtomicU64::new(0);
static EVM_NET_FAIL: AtomicU64 = AtomicU64::new(0);

static SOL_NET_OK: AtomicU64 = AtomicU64::new(0);
static SOL_NET_FAIL: AtomicU64 = AtomicU64::new(0);

static TRX_NET_OK: AtomicU64 = AtomicU64::new(0);
static TRX_NET_FAIL: AtomicU64 = AtomicU64::new(0);

fn is_valid_solana_pubkey_32(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
    let decoded = match bs58::decode(t).into_vec() {
        Ok(v) => v,
        Err(_) => {
            return false;
        }
    };
    decoded.len() == 32
}

fn native_symbol_for(network: &str) -> &str {
    match network {
        "eth" => "eth",
        "bnb" => "bnb",
        "matic" => "matic",
        "op" => "op",
        _ => network, 
    }
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

fn lamports_u128_to_sol_fixed_18(lamports: u128) -> String {
    u128_base_units_to_fixed_18(lamports, SOL_DECIMALS)
}

fn sun_u128_to_trx_fixed_18(sun: u128) -> String {
    u128_base_units_to_fixed_18(sun, TRX_DECIMALS)
}

fn sol_mints_from_request(req: &BalanceRequest) -> Vec<String> {
    req.contracts
        .iter()
        .find(|c| c.network_name == "sol")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default()
}

fn tron_contracts_from_request(req: &BalanceRequest) -> Vec<String> {
    req.contracts
        .iter()
        .find(|c| c.network_name == "trx")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default()
}

pub async fn run_worker(state: AppState) {
    let poll_ms = state.cfg.worker_poll_ms;

    tracing::info!(worker_enabled = state.cfg.worker_enabled, poll_ms = poll_ms, "worker started");

    loop {
        if !state.cfg.worker_enabled {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            continue;
        }

        match claim_next_job(&state).await {
            Ok(Some(request_key)) => {
                JOBS_CLAIMED.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    request_key = %request_key,
                    jobs_claimed = JOBS_CLAIMED.load(Ordering::Relaxed),
                    "claimed job"
                );

                let job_start = Instant::now();
                let res = process_job(&state, &request_key).await;

                match res {
                    Ok(_) => {
                        JOBS_DONE.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(
                            request_key = %request_key,
                            elapsed_ms = job_start.elapsed().as_millis(),
                            jobs_done = JOBS_DONE.load(Ordering::Relaxed),
                            jobs_failed = JOBS_FAILED.load(Ordering::Relaxed),
                            "job done"
                        );
                    }
                    Err(e) => {
                        JOBS_FAILED.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(
                            request_key = %request_key,
                            elapsed_ms = job_start.elapsed().as_millis(),
                            error = %e,
                            jobs_done = JOBS_DONE.load(Ordering::Relaxed),
                            jobs_failed = JOBS_FAILED.load(Ordering::Relaxed),
                            "job failed"
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

    let filter =
        doc! {
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

    Ok(
        doc_opt.and_then(|d|
            d
                .get_str("requestKey")
                .ok()
                .map(|s| s.to_string())
        )
    )
}

async fn process_job(state: &AppState, request_key: &str) -> Result<(), anyhow::Error> {
    let snapshots = state.mongo.db.collection::<bson::Document>("balance_snapshots");
    let now = DateTime::now();

    // Mark snapshot running
    snapshots.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "refreshState": "running", "isComplete": false } },
    ).await?;

    // Load snapshot doc
    // ✅ FIXED: Removed 'None' argument
    let snap = snapshots
        .find_one(doc! { "requestKey": request_key })
        .await?
        .ok_or_else(|| anyhow!("snapshot not found for requestKey"))?;

    // ✅ CAPTURE INITIAL STATE FOR CHANGE DETECTION
    let initial_result_json: serde_json::Value = snap
        .get("result")
        .cloned()
        .and_then(|b| bson::from_bson(b).ok())
        .unwrap_or(json!({}));

    let normalized_req_bson = snap.get("normalizedRequest").cloned().unwrap_or(bson::Bson::Null);

    let normalized_req_json: serde_json::Value = bson
        ::from_bson(normalized_req_bson)
        .unwrap_or_else(|_| json!({}));

    let req_raw: BalanceRequest = serde_json
        ::from_value(normalized_req_json.clone())
        .unwrap_or_else(|_| BalanceRequest {
            hard_refresh: false,
            contracts: vec![],
            wallet_addresses: vec![],
            solana_wallet_addresses: vec![],
            tron_wallet_addresses: vec![],
            doge_wallet_addresses: vec![],
            btc_wallet_addresses: vec![],
        });

    // Re-normalize inside worker
    let req: BalanceRequest = normalize_request(&req_raw);

    // Heal snapshot.normalizedRequest
    let req_sanitized_json = serde_json::to_value(&req).unwrap_or_else(|_| json!({}));

    // 1. Start with a fresh ZERO result shape (Contract Truth)
    let mut final_result = zero_result_from_request(&req);

    // Overlay existing DB result if present to prevent flashing zeroes.
    if let Some(existing_bson) = snap.get("result") {
        if existing_bson != &bson::Bson::Null {
            if let Ok(existing_json) = bson::from_bson::<serde_json::Value>(existing_bson.clone()) {
                final_result = existing_json;
            }
        }
    }

    // Row indices
    let evm_wallet_index: std::collections::HashMap<String, usize> = req.wallet_addresses
        .iter()
        .enumerate()
        .map(|(i, w)| (w.clone(), i))
        .collect();

    let sol_offset = req.wallet_addresses.len();
    let sol_wallet_index: std::collections::HashMap<String, usize> = req.solana_wallet_addresses
        .iter()
        .enumerate()
        .map(|(i, w)| (w.clone(), sol_offset + i))
        .collect();

    let tron_offset = sol_offset + req.solana_wallet_addresses.len();
    let tron_wallet_index: std::collections::HashMap<String, usize> = req.tron_wallet_addresses
        .iter()
        .enumerate()
        .map(|(i, w)| (w.clone(), tron_offset + i))
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

        let balances: EvmBalances = match
            fetch_balances_multicall3(
                &rpc,
                &req.wallet_addresses,
                &cg.contract_addresses,
                MAX_CALLS_PER_BATCH
            ).await
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

        let decimals_map = match
            fetch_token_decimals_multicall3(&rpc, &cg.contract_addresses, MAX_CALLS_PER_BATCH).await
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

            let row = data_arr.get_mut(row_idx).ok_or_else(|| anyhow!("wallet row missing"))?;

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
                let raw_bal = balances.erc20
                    .get(w)
                    .and_then(|m| m.get(token_addr))
                    .cloned()
                    .unwrap_or_default();

                let dec = decimals_map.get(token_addr).cloned().unwrap_or(18);
                let s = u256_to_decimal_string(raw_bal, dec, false);

                chain_obj.insert(token_addr.clone(), json!(s));
            }
        }

        // totals
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
            json!(u256_to_decimal_string(native_sum, NATIVE_DECIMALS, false))
        );

        for token_addr in &cg.contract_addresses {
            let mut sum = ethereum_types::U256::zero();

            for w in &req.wallet_addresses {
                let v = balances.erc20
                    .get(w)
                    .and_then(|m| m.get(token_addr))
                    .cloned()
                    .unwrap_or_default();
                sum += v;
            }

            let dec = decimals_map.get(token_addr).cloned().unwrap_or(18);
            total_chain_obj.insert(
                token_addr.clone(),
                json!(u256_to_decimal_string(sum, dec, false))
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

    // ✅ FIXED: Don't write 'result' during partial updates, only timestamp/heartbeat
    snapshots.update_one(
        doc! { "requestKey": request_key },
        doc! {
                "$set": {
                    "lastUpdatedAt": DateTime::now(),
                    "refreshState": "running",
                    "isComplete": false
                }
            }
    ).await?;
    tracing::debug!(request_key = %request_key, "incremental DB update (EVM done)");

    // ==========================
    // SOL processing
    // ==========================
    if !req.solana_wallet_addresses.is_empty() {
        let sol_start = Instant::now();
        let sol_rpc = SolanaRpcClient::new(
            state.cfg.solana_rpc_url.clone(),
            state.cfg.rpc_timeout_ms
        );

        let sol_mints = sol_mints_from_request(&req);

        let mut sol_total_lamports: u128 = 0;
        let mut spl_totals: std::collections::HashMap<
            String,
            u128
        > = std::collections::HashMap::new();
        let mut spl_decimals: std::collections::HashMap<
            String,
            u32
        > = std::collections::HashMap::new();

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

                let row = data_arr.get_mut(row_idx).ok_or_else(|| anyhow!("wallet row missing"))?;

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
                    json!(lamports_u128_to_sol_fixed_18(lamports as u128))
                );
            }

            for mint in &sol_mints {
                if !is_valid_solana_pubkey_32(mint) {
                    tracing::warn!(mint = %mint, "invalid sol mint in snapshot -> skipped");
                    continue;
                }

                let (amt_u128, dec_u32) = sol_rpc
                    .get_spl_balance_by_owner_mint(w, mint).await
                    .unwrap_or_else(|e| {
                        SOL_NET_FAIL.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(
                            wallet = %w,
                            mint = %mint,
                            error = %e,
                            "sol spl fetch failed -> keeping zero"
                        );
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
                let row = data_arr.get_mut(row_idx).ok_or_else(|| anyhow!("wallet row missing"))?;
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
            if
                let Some(sol_total_obj) = totals_balance_obj
                    .get_mut("sol")
                    .and_then(|v| v.as_object_mut())
            {
                sol_total_obj.insert(
                    "sol".to_string(),
                    json!(lamports_u128_to_sol_fixed_18(sol_total_lamports))
                );

                for mint in &sol_mints {
                    let sum = spl_totals.get(mint).cloned().unwrap_or(0u128);
                    let dec = spl_decimals.get(mint).cloned().unwrap_or(0u32);
                    sol_total_obj.insert(
                        mint.clone(),
                        json!(u128_base_units_to_fixed_18(sum, dec))
                    );
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

        // ✅ FIXED: Don't write 'result' during partial updates
        snapshots.update_one(
            doc! { "requestKey": request_key },
            doc! {
                    "$set": {
                        "lastUpdatedAt": DateTime::now(),
                        "refreshState": "running",
                        "isComplete": false
                    }
                }
        ).await?;
        tracing::debug!(request_key = %request_key, "incremental DB update (Solana done)");
    }

    // ==========================
    // TRON processing
    // ==========================
    let tron_contracts = tron_contracts_from_request(&req);
    let tron_requested = !req.tron_wallet_addresses.is_empty() || !tron_contracts.is_empty();

    if
        tron_requested &&
        (!req.tron_wallet_addresses.is_empty() || !req.wallet_addresses.is_empty())
    {
        let trx_start = Instant::now();

        let tron = TronRpcClient::new(
            state.cfg.tron_fullnode_url.clone(),
            state.cfg.tron_solidity_url.clone(),
            state.cfg.tron_api_key.clone(),
            state.cfg.rpc_timeout_ms
        );

        let mut dec_cache: std::collections::HashMap<
            String,
            u32
        > = std::collections::HashMap::new();

        let mut trx_total_sun: u128 = 0;
        let mut trc20_totals: std::collections::HashMap<
            String,
            u128
        > = std::collections::HashMap::new();

        let (wallets_for_tron, row_index_lookup, derived_from_evm): (
            Vec<String>,
            Box<dyn (Fn(&str) -> Result<usize, anyhow::Error>) + Send + Sync>,
            bool,
        ) = if !req.tron_wallet_addresses.is_empty() {
            let f = move |w: &str| {
                tron_wallet_index
                    .get(w)
                    .copied()
                    .ok_or_else(|| anyhow!("tron wallet index missing"))
            };
            (req.tron_wallet_addresses.clone(), Box::new(f), false)
        } else {
            let f = move |w: &str| {
                evm_wallet_index
                    .get(w)
                    .copied()
                    .ok_or_else(|| anyhow!("evm wallet index missing (for tron derived)"))
            };
            (req.wallet_addresses.clone(), Box::new(f), true)
        };

        let owner_for_calls = wallets_for_tron.first().cloned().unwrap_or_default();
        let owner_for_calls_b58 = if derived_from_evm {
            TronRpcClient::evm_hex_to_tron_base58(&owner_for_calls).unwrap_or_default()
        } else {
            owner_for_calls.clone()
        };

        for c in &tron_contracts {
            let d = tron.get_trc20_decimals(c, &owner_for_calls_b58).await.unwrap_or_else(|e| {
                tracing::error!(contract=%c, error=%e, "tron decimals fetch failed -> default 18");
                18u32
            });
            dec_cache.insert(c.clone(), d);
        }

        let mut valid_targets: Vec<(String, String, usize)> = Vec::with_capacity(
            wallets_for_tron.len()
        );

        for w in &wallets_for_tron {
            if let Ok(row_idx) = row_index_lookup(w) {
                let wallet_b58 = if derived_from_evm {
                    TronRpcClient::evm_hex_to_tron_base58(w).unwrap_or_default()
                } else {
                    w.clone()
                };

                if !wallet_b58.is_empty() {
                    valid_targets.push((w.clone(), wallet_b58, row_idx));
                }
            }
        }

        // Phase A: Native TRX (Concurrent)
        let native_results = stream
            ::iter(valid_targets.clone())
            .map(|(_orig_w, wallet_b58, row_idx)| {
                let tron_client = tron.clone();
                async move {
                    let sun = tron_client
                        .get_trx_balance_sun(&wallet_b58).await
                        .unwrap_or_else(|e| {
                            TRX_NET_FAIL.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(wallet=%wallet_b58, error=%e, "tron getaccount failed -> keeping zero");
                            0u64
                        });
                    (row_idx, sun)
                }
            })
            .buffer_unordered(TRON_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>().await;

        for (row_idx, sun) in native_results {
            trx_total_sun = trx_total_sun.saturating_add(sun as u128);

            // ✅ FIX 2: remove * dereference
            let data_arr = final_result
                .get_mut("data")
                .and_then(|v| v.as_array_mut())
                .unwrap();
            let row = data_arr.get_mut(row_idx).unwrap(); // no *
            let bal_obj = row
                .get_mut("balance")
                .and_then(|v| v.as_object_mut())
                .unwrap();

            if !bal_obj.contains_key("trx") {
                bal_obj.insert("trx".to_string(), json!({}));
            }
            let trx_obj = bal_obj
                .get_mut("trx")
                .and_then(|v| v.as_object_mut())
                .unwrap();

            trx_obj.insert("trx".to_string(), json!(sun_u128_to_trx_fixed_18(sun as u128)));
        }

        // Phase B: TRC20 Calls (Concurrent)
        let mut trc20_tasks = Vec::new();
        for (orig_w, wallet_b58, row_idx) in &valid_targets {
            for c in &tron_contracts {
                trc20_tasks.push((wallet_b58.clone(), c.clone(), orig_w.clone(), *row_idx));
            }
        }

        let trc20_results = stream
            ::iter(trc20_tasks)
            .map(|(wallet_b58, contract, _orig_w, row_idx)| {
                let tron_client = tron.clone();
                async move {
                    let amt = tron_client
                        .get_trc20_balance(&contract, &wallet_b58).await
                        .unwrap_or_else(|e| {
                            TRX_NET_FAIL.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(wallet=%wallet_b58, contract=%contract, error=%e, "trc20 failed");
                            0u128
                        });
                    (row_idx, contract, amt)
                }
            })
            .buffer_unordered(TRON_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>().await;

        for (row_idx, contract, amt) in trc20_results {
            *trc20_totals.entry(contract.clone()).or_insert(0u128) += amt;

            let dec = dec_cache.get(&contract).cloned().unwrap_or(18u32);
            let formatted = u128_base_units_to_fixed_18(amt, dec);

            // ✅ FIX 2: remove * dereference if present (though here row_idx was passed as usize, so it should be fine as just row_idx)
            let data_arr = final_result
                .get_mut("data")
                .and_then(|v| v.as_array_mut())
                .unwrap();
            let row = data_arr.get_mut(row_idx).unwrap();
            let bal_obj = row
                .get_mut("balance")
                .and_then(|v| v.as_object_mut())
                .unwrap();

            if !bal_obj.contains_key("trx") {
                bal_obj.insert("trx".to_string(), json!({}));
            }
            let trx_obj = bal_obj
                .get_mut("trx")
                .and_then(|v| v.as_object_mut())
                .unwrap();
            trx_obj.insert(contract, json!(formatted));
        }

        // totals
        {
            let totals_balance_obj = final_result
                .get_mut("total")
                .and_then(|v| v.as_object_mut())
                .and_then(|m| m.get_mut("balance"))
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| anyhow!("final_result.total.balance missing or not object"))?;

            if !totals_balance_obj.contains_key("trx") {
                totals_balance_obj.insert("trx".to_string(), json!({}));
            }

            if
                let Some(trx_total_obj) = totals_balance_obj
                    .get_mut("trx")
                    .and_then(|v| v.as_object_mut())
            {
                trx_total_obj.insert(
                    "trx".to_string(),
                    json!(sun_u128_to_trx_fixed_18(trx_total_sun))
                );

                for c in &tron_contracts {
                    let sum = trc20_totals.get(c).cloned().unwrap_or(0u128);
                    let dec = dec_cache.get(c).cloned().unwrap_or(18u32);
                    trx_total_obj.insert(c.clone(), json!(u128_base_units_to_fixed_18(sum, dec)));
                }
            }
        }

        TRX_NET_OK.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            elapsed_ms = trx_start.elapsed().as_millis(),
            trx_ok = TRX_NET_OK.load(Ordering::Relaxed),
            trx_fail = TRX_NET_FAIL.load(Ordering::Relaxed),
            contracts = tron_contracts.len(),
            wallets = wallets_for_tron.len(),
            derived_from_evm = derived_from_evm,
            "tron network processed (parallel)"
        );
    }

    // ✅ COMPARE CHANGE
    let has_changed = final_result != initial_result_json;

    // Final Update
    snapshots.update_one(
        doc! { "requestKey": request_key },
        doc! {
                "$set": {
                    "lastUpdatedAt": now,
                    "refreshState": "idle",
                    "isComplete": true,
                    "hasChanged": has_changed, // ✅ UPDATED
                    "result": bson::to_bson(&final_result).unwrap_or(bson::Bson::Null),
                    "normalizedRequest": bson::to_bson(&req_sanitized_json).unwrap_or(bson::Bson::Null)
                }
            }
    ).await?;

    // Mark job done
    let jobs = state.mongo.db.collection::<bson::Document>("balance_refresh_jobs");

    jobs.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "status": "done", "updatedAt": now } }
    ).await?;

    Ok(())
}

async fn mark_job_failed(state: &AppState, request_key: &str) -> Result<(), mongodb::error::Error> {
    let jobs = state.mongo.db.collection::<bson::Document>("balance_refresh_jobs");
    let now = DateTime::now();

    // ✅ FIXED: Removed 'None' argument
    let job = jobs.find_one(doc! { "requestKey": request_key }).await?;
    let attempts =
        job
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
        }
    ).await?;

    Ok(())
}