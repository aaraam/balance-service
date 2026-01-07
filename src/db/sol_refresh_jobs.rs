use crate::db::models::BalanceRefreshJobDoc;
use bson::{ doc, DateTime };
use mongodb::Collection;

pub fn sol_refresh_jobs_collection(db: &mongodb::Database) -> Collection<BalanceRefreshJobDoc> {
    db.collection::<BalanceRefreshJobDoc>("sol_balance_refresh_jobs")
}

pub async fn ensure_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    use mongodb::{ options::IndexOptions, IndexModel };

    let coll = sol_refresh_jobs_collection(db);

    let model = IndexModel::builder()
        .keys(doc! { "requestKey": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();

    coll.create_index(model).await?;
    Ok(())
}

/// Enqueue a refresh job if it doesn't exist.
/// If it exists and is `done` or `failed`, re-queue it.
/// If it's already `queued` or `running`, do nothing.
///
/// Returns:
/// - Ok(true) if we actually queued/re-queued a job
/// - Ok(false) if job already queued/running and we did nothing
pub async fn enqueue_or_requeue(
    db: &mongodb::Database,
    request_key: &str,
) -> Result<bool, mongodb::error::Error> {
    let coll = sol_refresh_jobs_collection(db);
    let now = DateTime::now();

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

    let requeue_result = coll.update_one(requeue_filter, requeue_update).upsert(false).await?;
    if requeue_result.modified_count == 1 {
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

    let insert_result = coll.update_one(insert_filter, insert_update).upsert(true).await?;
    Ok(insert_result.upserted_id.is_some())
}
