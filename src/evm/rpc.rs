use anyhow::anyhow;
use reqwest::Client;
use serde_json::json;

#[derive(Clone)]
pub struct RpcClient {
    http: Client,
    rpc_url: String,
}

impl RpcClient {
    pub fn new(rpc_url: String) -> Self {
        Self {
            http: Client::new(),
            rpc_url,
        }
    }

    pub async fn eth_call(&self, to: &str, data: &str) -> Result<String, anyhow::Error> {
        // thirdweb expects standard json-rpc
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": to,
                    "data": data
                },
                "latest"
            ]
        });

        let res = self
            .http
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await?;

        let status = res.status();
        let v: serde_json::Value = res.json().await?;

        if !status.is_success() {
            return Err(anyhow!("rpc http error: status={} body={}", status, v));
        }

        if let Some(err) = v.get("error") {
            return Err(anyhow!("rpc error: {}", err));
        }

        let result = v
            .get("result")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("missing rpc result: {}", v))?;

        Ok(result.to_string())
    }
}
