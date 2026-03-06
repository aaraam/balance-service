use crate::db::models::BalanceSnapshotDoc;
use bson::doc;
use mongodb::{options::FindOneOptions, Collection};
use serde::{Deserialize, Serialize};

/// Mini-struct for lightweight status polling.
/// Returned by `get_snapshot_status` — only fetches these fields from Mongo.
#[derive(Debug, Deserialize, Serialize)]
pub struct SnapshotStatus {
    #[serde(rename = "isComplete", default)]
    pub is_complete: bool,
    #[serde(rename = "hasChanged", default)]
    pub has_changed: bool,
    #[serde(rename = "requestKey")]
    pub request_key: String,
    /// Stage label written by the worker. May be absent on old snapshots.
    #[serde(rename = "progressStage")]
    pub progress_stage: Option<String>,
}

pub fn snapshots_collection(db: &mongodb::Database) -> Collection<BalanceSnapshotDoc> {
    db.collection::<BalanceSnapshotDoc>("balance_snapshots")
}

/// Create unique index on requestKey once
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

/// Fetch ONLY status fields (lightweight) using projection.
pub async fn get_snapshot_status(
    db: &mongodb::Database,
    request_key: &str,
) -> Result<Option<SnapshotStatus>, mongodb::error::Error> {
    let coll = db.collection::<SnapshotStatus>("balance_snapshots");

    let filter = doc! { "requestKey": request_key };

    let options = FindOneOptions::builder()
        .projection(doc! {
            "isComplete": 1,
            "hasChanged": 1,
            "requestKey": 1,
            "progressStage": 1,
            "_id": 0
        })
        .build();

    coll.find_one(filter).with_options(options).await
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
            "refreshState": "idle",
            "isComplete": false,
            "hasChanged": false,
            "progressStage": "queued"
        }
    };

    coll.update_one(filter, update).upsert(true).await?;

    Ok(())
}

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