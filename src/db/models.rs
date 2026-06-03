use bson::DateTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceSnapshotDoc {
    pub request_key: String,
    pub normalized_request: serde_json::Value,
    pub result: serde_json::Value,
    pub last_updated_at: DateTime,
    pub refresh_state: String,

    #[serde(default)]
    pub is_complete: bool,

    #[serde(default)]
    pub has_changed: bool,

    /// Tracks which phase the worker has completed.
    /// Values: "queued" | "evm_done" | "sol_done" | "complete"
    /// Absent on old snapshots — treated as None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_stage: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceRefreshJobDoc {
    pub request_key: String,
    pub status: String, // queued | running | done | failed
    pub attempts: i32,
    pub next_retry_at: Option<DateTime>,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}
