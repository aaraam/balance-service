use crate::db::models::BalanceRefreshJobDoc;
use bson::{doc, DateTime};
use mongodb::Collection;

pub fn refresh_jobs_collection(db: &mongodb::Database) -> Collection<BalanceRefreshJobDoc> {
    db.collection::<BalanceRefreshJobDoc>("balance_refresh_jobs")
}

pub async fn ensure_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    use mongodb::{options::IndexOptions, IndexModel};

    let coll = refresh_jobs_collection(db);

    let model = IndexModel::builder()
        .keys(doc! { "requestKey": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();

    coll.create_index(model).await?;

    // Worker claim helper index
    let model2 = IndexModel::builder()
        .keys(doc! { "status": 1, "nextRetryAt": 1, "updatedAt": -1 })
        .options(IndexOptions::builder().build())
        .build();

    coll.create_index(model2).await?;
    Ok(())
}

/// Enqueue a refresh job if it doesn't exist.
/// If it exists and is `done` or `failed`, re-queue it.
/// If it's already `queued` or `running`, do nothing.
pub async fn enqueue_or_requeue(
    db: &mongodb::Database,
    request_key: &str,
) -> Result<bool, mongodb::error::Error> {
    let coll = refresh_jobs_collection(db);
    let now = DateTime::now();

    tracing::debug!(request_key=%request_key, "enqueue_or_requeue called");

    let requeue_filter = doc! {
        "requestKey": request_key,
        "status": { "$in": ["done", "failed"] }
    };

    let requeue_update = doc! {
        "$set": {
            "status": "queued",
            "updatedAt": now,
            "nextRetryAt": bson::Bson::Null,
            "attempts": 0
        }
    };

    let requeue_result = coll
        .update_one(requeue_filter, requeue_update)
        .upsert(false)
        .await?;

    tracing::debug!(
        request_key=%request_key,
        matched=?requeue_result.matched_count,
        modified=?requeue_result.modified_count,
        "requeue attempt finished"
    );

    if requeue_result.modified_count == 1 {
        tracing::info!(request_key=%request_key, "job re-queued (was done/failed)");
        return Ok(true);
    }

    let insert_filter = doc! { "requestKey": request_key };
    let insert_update = doc! {
        "$setOnInsert": {
            "requestKey": request_key,
            "status": "queued",
            "attempts": 0,
            "nextRetryAt": bson::Bson::Null,
            "createdAt": now,
            "updatedAt": now
        }
    };

    let insert_result = coll
        .update_one(insert_filter, insert_update)
        .upsert(true)
        .await?;

    let did_insert = insert_result.upserted_id.is_some();

    tracing::info!(
        request_key=%request_key,
        did_insert=did_insert,
        matched=?insert_result.matched_count,
        modified=?insert_result.modified_count,
        "insert attempt finished"
    );

    Ok(did_insert)
}
