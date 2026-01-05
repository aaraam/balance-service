use crate::AppState;
use bson::{doc, DateTime};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};

pub async fn run_worker(state: AppState) {
    let poll_ms = state.cfg.worker_poll_ms;
    tracing::info!(
        worker_enabled = state.cfg.worker_enabled,
        poll_ms = poll_ms,
        // NOTE: only if you added worker_slow_ms in config.rs
        // worker_slow_ms = state.cfg.worker_slow_ms,
        "worker started"
    );

    loop {
        if !state.cfg.worker_enabled {
            tracing::debug!("worker disabled -> sleeping 1000ms");
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
                tracing::debug!(poll_ms = poll_ms, "no queued jobs -> sleeping");
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

    // Claim one job that is queued and either no nextRetryAt or nextRetryAt <= now
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

    if doc_opt.is_some() {
        tracing::debug!("claim_next_job: found a queued job");
    } else {
        tracing::debug!("claim_next_job: no queued jobs");
    }

    Ok(doc_opt.and_then(|d| d.get_str("requestKey").ok().map(|s| s.to_string())))
}

async fn process_job(state: &AppState, request_key: &str) -> Result<(), anyhow::Error> {
    tracing::info!(request_key = %request_key, "processing job started");

    // OPTIONAL: slow mode so you can actually SEE the pipeline happen.
    // Uncomment if you added worker_slow_ms to AppConfig.
    if state.cfg.worker_slow_ms > 0 {
        tracing::debug!(
            request_key = %request_key,
            slow_ms = state.cfg.worker_slow_ms,
            "slow mode enabled -> sleeping before processing"
        );
        tokio::time::sleep(std::time::Duration::from_millis(state.cfg.worker_slow_ms)).await;
    }

    let snapshots = state
        .mongo
        .db
        .collection::<bson::Document>("balance_snapshots");
    let now = DateTime::now();

    // 1) Mark snapshot running
    snapshots
        .update_one(
            doc! { "requestKey": request_key },
            doc! { "$set": { "refreshState": "running" } },
        )
        .await?;
    tracing::debug!(request_key = %request_key, "snapshot refreshState set to running");

    // 2) Dummy refresh marker (proves pipeline works; later replace with real RPC refresh)
    let marker = format!("dummy-refresh-{}", now.timestamp_millis());

    snapshots
        .update_one(
            doc! { "requestKey": request_key },
            doc! {
                "$set": {
                    "lastUpdatedAt": now,
                    "refreshState": "idle",
                    "result.total.balance.__refresh_marker": &marker
                }
            },
        )
        .await?;
    tracing::info!(request_key = %request_key, marker = %marker, "snapshot updated with dummy marker");

    // 3) Mark job done
    let jobs = state
        .mongo
        .db
        .collection::<bson::Document>("balance_refresh_jobs");

    jobs.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "status": "done", "updatedAt": now } },
    )
    .await?;
    tracing::info!(request_key = %request_key, "job marked done");

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

    // backoff: attempts * 5 seconds
    let backoff_secs = (attempts as i64) * 5;
    let next_retry_ms = now.timestamp_millis() + backoff_secs * 1000;
    let next_retry = DateTime::from_millis(next_retry_ms);

    tracing::warn!(
        request_key = %request_key,
        attempts = attempts,
        backoff_secs = backoff_secs,
        next_retry_ms = next_retry_ms,
        "marking job failed -> re-queue with backoff"
    );

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
