use anyhow::anyhow;
use reqwest::Client;
use serde_json::json;

#[derive(Clone)]
pub struct SolanaRpcClient {
    http: Client,
    rpc_url: String,
}

impl SolanaRpcClient {
    pub fn new(rpc_url: String) -> Self {
        Self {
            http: Client::new(),
            rpc_url,
        }
    }

    pub async fn get_balance_lamports(&self, pubkey: &str) -> Result<u64, anyhow::Error> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [ pubkey ]
        });

        let res = self.http.post(&self.rpc_url).json(&payload).send().await?;
        let status = res.status();
        let v: serde_json::Value = res.json().await?;

        if !status.is_success() {
            return Err(anyhow!("sol rpc http error: status={} body={}", status, v));
        }
        if let Some(err) = v.get("error") {
            return Err(anyhow!("sol rpc error: {}", err));
        }

        let lamports = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|x| x.as_u64())
            .ok_or_else(|| anyhow!("missing sol getBalance result.value: {}", v))?;

        Ok(lamports)
    }
}
