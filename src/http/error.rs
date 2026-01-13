// ==================================================
// balance-service\src\http\error.rs
// ==================================================

use axum::{ http::StatusCode, response::{ IntoResponse, Response }, Json };
use serde::{ Deserialize, Serialize };
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ApiErrorBody,
}

impl ApiError {
    pub fn bad_request(
        code: impl Into<String>,
        message: impl Into<String>,
        details: Option<Value>
    ) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ApiErrorBody {
                code: code.into(),
                message: message.into(),
                details,
            },
        }
    }

    pub fn too_many_requests(
        code: impl Into<String>,
        message: impl Into<String>,
        details: Option<Value>
    ) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: ApiErrorBody {
                code: code.into(),
                message: message.into(),
                details,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let payload = crate::http::dto::BalanceResponse {
            status: false,
            result: serde_json::json!({}),
            error: Some(self.body),
        };

        (self.status, Json(payload)).into_response()
    }
}
