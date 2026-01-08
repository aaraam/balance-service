// ==================================================
// balance-service\src\worker\runner.rs
// ==================================================

use crate::AppState;
use anyhow::anyhow;
use bson::{doc, DateTime};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use serde_json::json;

use crate::chains::{is_ignored_network, supported_evm_networks};
use crate::evm::format::u256_to_decimal_string;
use crate::evm::multicall3::{
    fetch_balances_multicall3, fetch_token_decimals_multicall3, EvmBalances,
};
use crate::evm::rpc::RpcClient;
use crate::http::dto::{zero_result_from_request, BalanceRequest};
use crate::solana::rpc::SolanaRpcClient;

const MAX_CALLS_PER_BATCH: usize = 600;

/// IMPORTANT: fixed 18-decimal strings for native too (NO trim)
const NATIVE_DECIMALS: u32 = 18;

// SOL uses 9 decimals (lamports). We output 18-decimal fixed strings.
const SOL_DECIMALS: u32 = 9;

fn native_symbol_for(network: &str) -> &str {
    match network {
        "eth" => "eth",
        "bnb" => "bnb",
        "matic" => "matic",
        "op" => "op",
        _ => network, // avax/ftm/cro/etc
    }
}

// Convert lamports -> fixed 18-decimal SOL string.
// Example: 1 lamport => 0.000000001000000000
fn lamports_to_sol_fixed_18(lamports: u64) -> String {
    // base-decimals = 9
    let whole = lamports / 1_000_000_000;
    let frac = lamports % 1_000_000_000;

    let mut frac_str = frac.to_string();
    if frac_str.len() < (SOL_DECIMALS as usize) {
        let pad = "0".repeat((SOL_DECIMALS as usize) - frac_str.len());
        frac_str = format!("{}{}", pad, frac_str);
    }

    // now pad to 18 decimals by adding zeros to the right
    // (keeps numeric value identical, just higher fixed precision)
    let extra = (18 - SOL_DECIMALS) as usize;
    frac_str.push_str(&"0".repeat(extra));

    format!("{}.{}", whole, frac_str)
}

pub async fn run_worker(state: AppState) {
    let poll_ms = state.cfg.worker_poll_ms;

    tracing::info!(
        worker_enabled = state.cfg.worker_enabled,
        poll_ms = poll_ms,
        "worker started"
    );

    loop {
        if !state.cfg.worker_enabled {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            continue;
        }

        match claim_next_job(&state).await {
            Ok(Some(request_key)) => {
                tracing::info!(request_key = %request_key, "claimed job");

                let res = process_job(&state, &request_key).await;
                if let Err(e) = res {
                    tracing::error!(request_key = %request_key, error = %e, "job failed");
                    let _ = mark_job_failed(&state, &request_key).await;
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
    let coll = state
        .mongo
        .db
        .collection::<bson::Document>("balance_refresh_jobs");
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

    let doc_opt = coll
        .find_one_and_update(filter, update)
        .with_options(opts)
        .await?;

    Ok(doc_opt.and_then(|d| d.get_str("requestKey").ok().map(|s| s.to_string())))
}

async fn process_job(state: &AppState, request_key: &str) -> Result<(), anyhow::Error> {
    let snapshots = state
        .mongo
        .db
        .collection::<bson::Document>("balance_snapshots");
    let now = DateTime::now();

    // Mark snapshot running
    snapshots
        .update_one(
            doc! { "requestKey": request_key },
            doc! { "$set": { "refreshState": "running" } },
        )
        .await?;

    // Load normalized request
    let snap = snapshots
        .find_one(doc! { "requestKey": request_key })
        .await?
        .ok_or_else(|| anyhow!("snapshot not found for requestKey"))?;

    let normalized_req_bson = snap
        .get("normalizedRequest")
        .cloned()
        .unwrap_or(bson::Bson::Null);

    let normalized_req_json: serde_json::Value =
        bson::from_bson(normalized_req_bson).unwrap_or_else(|_| json!({}));

    let req: BalanceRequest =
        serde_json::from_value(normalized_req_json.clone()).unwrap_or_else(|_| BalanceRequest {
            hard_refresh: false,
            contracts: vec![],
            wallet_addresses: vec![],
            solana_wallet_addresses: vec![],
            doge_wallet_addresses: vec![],
            btc_wallet_addresses: vec![],
        });

    // Build fully-shaped ZERO result FIRST (contract truth)
    let mut final_result = zero_result_from_request(&req);

    // Row indices: data = [evm rows..., sol rows...]
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
    // EVM processing (unchanged)
    // ==========================
    let evm_map = supported_evm_networks();

    for cg in &req.contracts {
        let net = cg.network_name.as_str();

        if is_ignored_network(net) {
            tracing::warn!(network=%net, "ignored network in contracts list");
            continue;
        }

        let Some(chain) = evm_map.get(net).copied() else {
            tracing::warn!(network=%net, "unsupported network in contracts list (ignored)");
            continue;
        };

        let rpc_url = chain.thirdweb_rpc_url(&state.cfg.thirdweb_client_id);
        let rpc = RpcClient::new(rpc_url);

        let balances: EvmBalances = fetch_balances_multicall3(
            &rpc,
            &req.wallet_addresses,
            &cg.contract_addresses,
            MAX_CALLS_PER_BATCH,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::error!(network=%net, error=%e, "evm fetch failed -> keeping zeros");
            EvmBalances {
                native: Default::default(),
                erc20: Default::default(),
            }
        });

        let decimals_map =
            fetch_token_decimals_multicall3(&rpc, &cg.contract_addresses, MAX_CALLS_PER_BATCH)
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(network=%net, error=%e, "decimals fetch failed -> default 18");
                    std::collections::HashMap::new()
                });

        for w in &req.wallet_addresses {
            let row_idx = *evm_wallet_index
                .get(w)
                .ok_or_else(|| anyhow!("evm wallet index missing"))?;

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
        }

        let mut native_sum = ethereum_types::U256::zero();
        for w in &req.wallet_addresses {
            native_sum += balances.native.get(w).cloned().unwrap_or_default();
        }

        {
            let totals_balance_obj = final_result
                .get_mut("total")
                .and_then(|v| v.as_object_mut())
                .and_then(|m| m.get_mut("balance"))
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| anyhow!("final_result.total.balance missing or not object"))?;

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
        }
    }

    // ==========================
    // SOL processing (Phase 6)
    // ==========================
    if !req.solana_wallet_addresses.is_empty() {
        let sol_rpc = SolanaRpcClient::new(state.cfg.solana_rpc_url.clone());

        let mut sol_total_lamports: u128 = 0;

        for w in &req.solana_wallet_addresses {
            let row_idx = *sol_wallet_index
                .get(w)
                .ok_or_else(|| anyhow!("sol wallet index missing"))?;

            let lamports = sol_rpc.get_balance_lamports(w).await.unwrap_or_else(|e| {
                tracing::error!(wallet=%w, error=%e, "sol getBalance failed -> keeping zero");
                0u64
            });

            sol_total_lamports += lamports as u128;

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

                let sol_obj = bal_obj
                    .get_mut("sol")
                    .and_then(|v| v.as_object_mut())
                    .ok_or_else(|| anyhow!("balance.sol missing or not object"))?;

                sol_obj.insert("sol".to_string(), json!(lamports_to_sol_fixed_18(lamports)));
            }
        }

        // total.balance.sol.sol
        {
            let totals_balance_obj = final_result
                .get_mut("total")
                .and_then(|v| v.as_object_mut())
                .and_then(|m| m.get_mut("balance"))
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| anyhow!("final_result.total.balance missing or not object"))?;

            if let Some(sol_total_obj) = totals_balance_obj
                .get_mut("sol")
                .and_then(|v| v.as_object_mut())
            {
                // sol_total_lamports fits in u64 realistically, but we store u128 here
                let lamports_u64 = sol_total_lamports.min(u64::MAX as u128) as u64;
                sol_total_obj.insert(
                    "sol".to_string(),
                    json!(lamports_to_sol_fixed_18(lamports_u64)),
                );
            }
        }
    }

    // Update snapshot (final_result is fully-shaped)
    snapshots
        .update_one(
            doc! { "requestKey": request_key },
            doc! {
                "$set": {
                    "lastUpdatedAt": now,
                    "refreshState": "idle",
                    "result": bson::to_bson(&final_result).unwrap_or(bson::Bson::Null)
                }
            },
        )
        .await?;

    // Mark job done
    let jobs = state
        .mongo
        .db
        .collection::<bson::Document>("balance_refresh_jobs");

    jobs.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "status": "done", "updatedAt": now } },
    )
    .await?;

    Ok(())
}

async fn mark_job_failed(state: &AppState, request_key: &str) -> Result<(), mongodb::error::Error> {
    let jobs = state
        .mongo
        .db
        .collection::<bson::Document>("balance_refresh_jobs");
    let now = DateTime::now();

    let job = jobs.find_one(doc! { "requestKey": request_key }).await?;
    let attempts = job
        .as_ref()
        .and_then(|d| d.get_i32("attempts").ok())
        .unwrap_or(0)
        + 1;

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
    )
    .await?;

    Ok(())
}
