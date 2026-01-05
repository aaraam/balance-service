use crate::db::models::BalanceSnapshotDoc;
use bson::doc;
use mongodb::Collection;

pub fn snapshots_collection(db: &mongodb::Database) -> Collection<BalanceSnapshotDoc> {
    db.collection::<BalanceSnapshotDoc>("balance_snapshots")
}

/// Create unique index on requestKey once (call at startup in Phase 2+).
pub async fn ensure_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    use mongodb::options::IndexOptions;
    use mongodb::IndexModel;

    let coll = snapshots_collection(db);
    let model = IndexModel::builder()
        .keys(doc! { "requestKey": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();

    coll.create_index(model).await?;
    Ok(())
}

pub async fn get_snapshot(
    db: &mongodb::Database,
    request_key: &str,
) -> Result<Option<BalanceSnapshotDoc>, mongodb::error::Error> {
    let coll = snapshots_collection(db);
    let filter = doc! { "requestKey": request_key };
    coll.find_one(filter).await
}

pub async fn upsert_empty_snapshot(
    db: &mongodb::Database,
    request_key: &str,
    normalized_request_json: serde_json::Value,
    empty_result_json: serde_json::Value,
) -> Result<(), mongodb::error::Error> {
    let coll = snapshots_collection(db);
    let filter = doc! { "requestKey": request_key };

    let now = bson::DateTime::now();

    let update = doc! {
        "$setOnInsert": {
            "requestKey": request_key,
            "normalizedRequest": bson::to_bson(&normalized_request_json)
                .unwrap_or(bson::Bson::Null),
            "result": bson::to_bson(&empty_result_json)
                .unwrap_or(bson::Bson::Null),
            "lastUpdatedAt": now,
            "refreshState": "idle"
        }
    };

    coll.update_one(filter, update).upsert(true).await?;

    Ok(())
}

/// ===============================
/// NEW — Phase 4 helper
/// Keeps snapshot state consistent
/// ===============================
pub async fn set_refresh_state(
    db: &mongodb::Database,
    request_key: &str,
    state: &str,
) -> Result<(), mongodb::error::Error> {
    let coll = snapshots_collection(db);

    coll.update_one(
        doc! { "requestKey": request_key },
        doc! { "$set": { "refreshState": state } },
    )
    .await?;

    Ok(())
}
