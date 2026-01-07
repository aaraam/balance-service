use crate::AppState;
use anyhow::anyhow;
use bson::{ doc, DateTime };
use mongodb::options::{ FindOneAndUpdateOptions, ReturnDocument };
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::db::{ sol_refresh_jobs, sol_snapshots };
use crate::http::dto::BalanceRequest;
use crate::sol::format::u128_to_decimal_string;
use crate::sol::rpc::SolRpcClient;

const SOL_DECIMALS: u32 = 9;

// Timeouts (tune if needed)
const NATIVE_TIMEOUT_MS: u64 = 2500;
const SPL_TIMEOUT_MS: u64 = 2500;

#[derive(Debug)]
struct WalletSolResult {
    owner: String,
    lamports: u64,
    spl: Vec<(String /*mint*/, u128 /*amt*/, u32 /*dec*/)>,
    native_ok: bool,
}

pub async fn run_sol_worker(state: AppState) {
    let poll_ms = state.cfg.worker_poll_ms;

    tracing::info!(
        worker_enabled = state.cfg.worker_enabled,
        poll_ms = poll_ms,
        wallet_parallelism = state.cfg.sol_worker_wallet_parallelism,
        rpc_concurrency = state.cfg.sol_worker_rpc_concurrency,
        "sol worker started"
    );

    loop {
        if !state.cfg.worker_enabled {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            continue;
        }

        match claim_next_sol_job(&state).await {
            Ok(Some(request_key)) => {
                tracing::info!(request_key = %request_key, "sol claimed job");

                let res = process_sol_job(&state, &request_key).await;
                if let Err(e) = res {
                    tracing::error!(request_key = %request_key, error = %e, "sol job failed");
                    let _ = mark_sol_job_failed(&state, &request_key).await;
                }
            }
            Ok(None) => {
                tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
            }
            Err(e) => {
                tracing::error!(error = %e, "sol worker claim error -> sleeping 1000ms");
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            }
        }
    }
}

async fn claim_next_sol_job(state: &AppState) -> Result<Option<String>, mongodb::error::Error> {
    // Typed collection: BalanceRefreshJobDoc
    let coll = sol_refresh_jobs::sol_refresh_jobs_collection(&state.mongo.db);
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

    // FIX: no get_str; it's a struct field
    Ok(doc_opt.map(|d| d.request_key))
}

async fn process_sol_job(state: &AppState, request_key: &str) -> Result<(), anyhow::Error> {
    let now = DateTime::now();

    sol_snapshots::set_refresh_state(&state.mongo.db, request_key, "running").await?;

    // Load normalized request
    let snap = sol_snapshots::get_snapshot(&state.mongo.db, request_key).await?
        .ok_or_else(|| anyhow!("sol snapshot not found for requestKey"))?;

    let normalized_req_json = snap.normalized_request.clone();

    let req: BalanceRequest = serde_json::from_value(normalized_req_json.clone())
        .unwrap_or_else(|_| BalanceRequest {
            hard_refresh: false,
            contracts: vec![],
            wallet_addresses: vec![],
            solana_wallet_addresses: vec![],
            doge_wallet_addresses: vec![],
            btc_wallet_addresses: vec![],
        });

    if req.solana_wallet_addresses.is_empty() {
        // Nothing to do
        sol_snapshots::set_refresh_state(&state.mongo.db, request_key, "idle").await?;
        let coll = sol_refresh_jobs::sol_refresh_jobs_collection(&state.mongo.db);
        coll.update_one(
            doc! { "requestKey": request_key },
            doc! { "$set": { "status": "done", "updatedAt": now } }
        ).await?;
        return Ok(());
    }

    let sol_mints: Vec<String> = req.contracts
        .iter()
        .find(|c| c.network_name == "sol")
        .map(|c| c.contract_addresses.clone())
        .unwrap_or_default();

    // Shared RPC + concurrency guard
    let rpc = SolRpcClient::new(state.cfg.solana_rpc_url.clone());
    let rpc = Arc::new(rpc);

    let wallet_parallelism = state.cfg.sol_worker_wallet_parallelism.max(1);
    let rpc_sem = Arc::new(Semaphore::new(state.cfg.sol_worker_rpc_concurrency.max(1)));

    // Start from zeros; fill SOL rows + totals only
    let mut result = crate::http::dto::zero_legacy_result_from_request(&req);

    let data_arr = result
        .get_mut("data")
        .and_then(|v| v.as_array_mut())
        .ok_or_else(|| anyhow!("result.data missing"))?;

    // walletAddress -> row index
    let mut row_index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (i, row) in data_arr.iter().enumerate() {
        if let Some(wa) = row.get("walletAddress").and_then(|x| x.as_str()) {
            row_index.insert(wa.to_string(), i);
        }
    }

    // ---- Fetch wallets in parallel (bounded) ----
    let owners = req.solana_wallet_addresses.clone();
    let mut joinset: JoinSet<WalletSolResult> = JoinSet::new();

    let mut in_flight: usize = 0;
    let mut results: Vec<WalletSolResult> = Vec::with_capacity(owners.len());

    for owner in owners {
        while in_flight >= wallet_parallelism {
            if let Some(res) = joinset.join_next().await {
                in_flight -= 1;
                if let Ok(v) = res {
                    results.push(v);
                }
            }
        }

        let rpc = rpc.clone();
        let rpc_sem = rpc_sem.clone();
        let mints = sol_mints.clone();
        joinset.spawn(async move { fetch_one_wallet(rpc, rpc_sem, owner, mints).await });

        in_flight += 1;
    }

    while let Some(res) = joinset.join_next().await {
        if in_flight > 0 {
            in_flight -= 1;
        }
        if let Ok(v) = res {
            results.push(v);
        }
    }

    // ---- Apply into JSON + compute totals ----
    let mut total_lamports: u128 = 0;
    let mut mint_totals: std::collections::HashMap<String, u128> = std::collections::HashMap::new();
    let mut mint_decimals: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

    for wr in results {
        total_lamports = total_lamports.saturating_add(wr.lamports as u128);

        if let Some(idx) = row_index.get(&wr.owner).copied() {
            let row = data_arr.get_mut(idx).ok_or_else(|| anyhow!("row missing"))?;

            let bal_obj = row
                .get_mut("balance")
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| anyhow!("balance not object"))?;

            let sol_entry = bal_obj.entry("sol".to_string()).or_insert_with(|| json!({}));
            let sol_obj = sol_entry
                .as_object_mut()
                .ok_or_else(|| anyhow!("balance.sol not object"))?;

            sol_obj.insert(
                "sol".to_string(),
                json!(u128_to_decimal_string(wr.lamports as u128, SOL_DECIMALS, true))
            );

            for (mint, amt, dec) in wr.spl {
                mint_totals.entry(mint.clone())
                    .and_modify(|x| *x = x.saturating_add(amt))
                    .or_insert(amt);

                mint_decimals.entry(mint.clone())
                    .and_modify(|d| if *d == 0 && dec != 0 { *d = dec })
                    .or_insert(dec);

                sol_obj.insert(mint, json!(u128_to_decimal_string(amt, dec, false)));
            }
        }
    }

    // totals.balance.sol
    let totals_obj = result
        .get_mut("total")
        .and_then(|v| v.get_mut("balance"))
        .and_then(|v| v.as_object_mut())
        .ok_or_else(|| anyhow!("total.balance missing"))?;

    let sol_total_entry = totals_obj.entry("sol".to_string()).or_insert_with(|| json!({}));
    let sol_total_obj = sol_total_entry
        .as_object_mut()
        .ok_or_else(|| anyhow!("total.balance.sol not object"))?;

    sol_total_obj.insert(
        "sol".to_string(),
        json!(u128_to_decimal_string(total_lamports, SOL_DECIMALS, true))
    );

    for mint in &sol_mints {
        let sum = mint_totals.get(mint).cloned().unwrap_or(0);
        let dec = mint_decimals.get(mint).cloned().unwrap_or(0);
        sol_total_obj.insert(mint.clone(), json!(u128_to_decimal_string(sum, dec, false)));
    }

    // Persist sol snapshot result
    sol_snapshots::update_result(&state.mongo.db, request_key, now, result).await?;

    // Mark sol job done
    let coll = sol_refresh_jobs::sol_refresh_jobs_collection(&state.mongo.db);
    coll.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "status": "done", "updatedAt": now } }
    ).await?;

    Ok(())
}

async fn fetch_one_wallet(
    rpc: Arc<SolRpcClient>,
    rpc_sem: Arc<Semaphore>,
    owner: String,
    mints: Vec<String>
) -> WalletSolResult {
    // Native SOL
    let (lamports, native_ok) = {
        let permit = rpc_sem.acquire().await;
        if permit.is_err() {
            (0u64, false)
        } else {
            let _permit = permit.unwrap();
            match tokio::time::timeout(
                std::time::Duration::from_millis(NATIVE_TIMEOUT_MS),
                rpc.get_balance_lamports(&owner)
            ).await {
                Ok(Ok(v)) => (v, true),
                Ok(Err(e)) => {
                    tracing::error!(owner=%owner, error=%e, "sol getBalance failed -> 0");
                    (0, false)
                }
                Err(_) => {
                    tracing::error!(owner=%owner, "sol getBalance timeout -> 0");
                    (0, false)
                }
            }
        }
    };

    let mut spl: Vec<(String, u128, u32)> = Vec::with_capacity(mints.len());

    // SPL mints (sequential per wallet, but globally bounded by rpc_sem)
    for mint in mints {
        let (amt, dec) = {
            let permit = rpc_sem.acquire().await;
            if permit.is_err() {
                (0u128, 0u32)
            } else {
                let _permit = permit.unwrap();
                match tokio::time::timeout(
                    std::time::Duration::from_millis(SPL_TIMEOUT_MS),
                    rpc.get_spl_balance_base_units(&owner, &mint)
                ).await {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => {
                        tracing::error!(owner=%owner, mint=%mint, error=%e, "spl balance fetch failed -> 0");
                        (0u128, 0u32)
                    }
                    Err(_) => {
                        tracing::error!(owner=%owner, mint=%mint, "spl balance timeout -> 0");
                        (0u128, 0u32)
                    }
                }
            }
        };

        spl.push((mint, amt, dec));
    }

    WalletSolResult {
        owner,
        lamports,
        spl,
        native_ok,
    }
}

async fn mark_sol_job_failed(
    state: &AppState,
    request_key: &str
) -> Result<(), mongodb::error::Error> {
    let coll = sol_refresh_jobs::sol_refresh_jobs_collection(&state.mongo.db);
    let now = DateTime::now();

    let job = coll.find_one(doc! { "requestKey": request_key }).await?;
    let attempts = job.as_ref().map(|d| d.attempts + 1).unwrap_or(1);

    let backoff_secs = (attempts as i64) * 5;
    let next_retry_ms = now.timestamp_millis() + backoff_secs * 1000;
    let next_retry = DateTime::from_millis(next_retry_ms);

    coll.update_one(
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
