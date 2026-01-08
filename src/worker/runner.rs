// ==================================================
// FILE: src/worker/runner.rs
// ==================================================

use crate::AppState;
use bson::{doc, DateTime};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use serde_json::json;

use crate::chains::{is_non_evm_stub, supported_evm_networks};
use crate::evm::format::u256_to_decimal_string;
use crate::evm::multicall3::{
    fetch_balances_multicall3, fetch_token_decimals_multicall3, EvmBalances,
};
use crate::evm::rpc::RpcClient;
use crate::http::dto::BalanceRequest;

const MAX_CALLS_PER_BATCH: usize = 600;

fn native_symbol_for(network: &str) -> &str {
    match network {
        "eth" => "eth",
        "bnb" => "bnb",
        "matic" => "matic",
        "op" => "op",
        _ => network, // avax/ftm/cro/etc
    }
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
        .ok_or_else(|| anyhow::anyhow!("snapshot not found for requestKey"))?;

    let normalized_req_bson = snap
        .get("normalizedRequest")
        .cloned()
        .unwrap_or(bson::Bson::Null);

    let normalized_req_json: serde_json::Value =
        bson::from_bson(normalized_req_bson).unwrap_or_else(|_| json!({}));

    let req: BalanceRequest = serde_json::from_value(normalized_req_json.clone()).unwrap_or_else(
        |_| BalanceRequest {
            hard_refresh: false,
            contracts: vec![],
            wallet_addresses: vec![],
            solana_wallet_addresses: vec![],
            doge_wallet_addresses: vec![],
            btc_wallet_addresses: vec![],
        },
    );

    // === Build results in your target shape ===
    // data: [{ walletAddress, balance: { chain: { nativeSymbol: "...", tokenAddr: "..." }}}]
    let mut data_arr: Vec<serde_json::Value> = vec![];
    let mut totals: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

    // Pre-init wallet rows (EVM only for now)
    for w in &req.wallet_addresses {
        data_arr.push(json!({
            "walletAddress": w,
            "balance": {}
        }));
    }

    let wallet_index: std::collections::HashMap<String, usize> = req
        .wallet_addresses
        .iter()
        .enumerate()
        .map(|(i, w)| (w.clone(), i))
        .collect();

    let evm_map = supported_evm_networks();

    for cg in &req.contracts {
        let net = cg.network_name.as_str();

        if let Some(chain) = evm_map.get(net).copied() {
            let rpc_url = chain.thirdweb_rpc_url(&state.cfg.thirdweb_client_id);
            let rpc = RpcClient::new(rpc_url);

            // 1) Fetch balances (native + ERC20 balanceOf) via Multicall3
            let balances: EvmBalances = fetch_balances_multicall3(
                &rpc,
                &req.wallet_addresses,
                &cg.contract_addresses,
                MAX_CALLS_PER_BATCH,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::error!(network=%net, error=%e, "evm fetch failed -> returning zeros");
                EvmBalances {
                    native: Default::default(),
                    erc20: Default::default(),
                }
            });

            // 2) Fetch decimals once per chain for all tokens (also via Multicall3)
            let decimals_map = fetch_token_decimals_multicall3(
                &rpc,
                &cg.contract_addresses,
                MAX_CALLS_PER_BATCH,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::error!(network=%net, error=%e, "decimals fetch failed -> default 18");
                std::collections::HashMap::new()
            });

            // Fill per-wallet results
            for w in &req.wallet_addresses {
                let native_wei = balances.native.get(w).cloned().unwrap_or_default();

                // native: 18 decimals, trimmed
                let native_str = u256_to_decimal_string(native_wei, 18, true);

                let token_map = balances.erc20.get(w).cloned().unwrap_or_default();

                // chain object holds: { nativeSymbol: "...", tokenAddr: "..." }
                let mut chain_obj = serde_json::Map::new();
                chain_obj.insert(
                    native_symbol_for(net).to_string(),
                    json!(native_str),
                );

                for (token_addr, raw_bal) in token_map {
                    let dec = decimals_map.get(&token_addr).cloned().unwrap_or(18);
                    let s = u256_to_decimal_string(raw_bal, dec, false);
                    chain_obj.insert(token_addr, json!(s));
                }

                let row_idx = *wallet_index
                    .get(w)
                    .ok_or_else(|| anyhow::anyhow!("wallet index missing"))?;
                let row = data_arr
                    .get_mut(row_idx)
                    .ok_or_else(|| anyhow::anyhow!("wallet row missing"))?;

                let bal_obj = row
                    .get_mut("balance")
                    .and_then(|v| v.as_object_mut())
                    .ok_or_else(|| anyhow::anyhow!("balance field not an object"))?;

                bal_obj.insert(net.to_string(), json!(chain_obj));
            }

            // Totals for this chain
            let mut native_sum = ethereum_types::U256::zero();
            let mut token_sums: std::collections::HashMap<String, ethereum_types::U256> =
                std::collections::HashMap::new();

            for w in &req.wallet_addresses {
                native_sum += balances.native.get(w).cloned().unwrap_or_default();

                if let Some(tm) = balances.erc20.get(w) {
                    for (t, v) in tm {
                        *token_sums
                            .entry(t.clone())
                            .or_insert_with(ethereum_types::U256::zero) += *v;
                    }
                }
            }

            let mut total_chain_obj = serde_json::Map::new();
            total_chain_obj.insert(
                native_symbol_for(net).to_string(),
                json!(u256_to_decimal_string(native_sum, 18, true)),
            );

            for (t, v) in token_sums {
                let dec = decimals_map.get(&t).cloned().unwrap_or(18);
                total_chain_obj.insert(t, json!(u256_to_decimal_string(v, dec, false)));
            }

            totals.insert(net.to_string(), json!(total_chain_obj));
        } else if is_non_evm_stub(net) {
            // keep as stub until you implement TRX/SOL/BTC/DOGE fetchers in THIS service
            totals.insert(
                net.to_string(),
                json!({
                    "stub": true
                }),
            );
        } else {
            totals.insert(
                net.to_string(),
                json!({
                    "stub": true,
                    "reason": "unsupported_network"
                }),
            );
        }
    }

    // Final result: matches your sample shape (no marker)
    let final_result = json!({
        "data": data_arr,
        "total": {
            "balance": totals
        }
    });

    // Update snapshot
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
