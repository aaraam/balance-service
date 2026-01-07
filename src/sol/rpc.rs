use anyhow::anyhow;
use reqwest::Client;
use serde_json::{ json, Value };

#[derive(Clone)]
pub struct SolRpcClient {
    http: Client,
    rpc_url: String,
}

impl SolRpcClient {
    pub fn new(rpc_url: String) -> Self {
        Self {
            http: Client::new(),
            rpc_url,
        }
    }

    /// getBalance(owner) -> lamports
    pub async fn get_balance_lamports(&self, owner: &str) -> Result<u64, anyhow::Error> {
        let payload =
            json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [ owner ]
        });

        let v = self.rpc_call(payload).await?;
        let lamports = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|x| x.as_u64())
            .ok_or_else(|| anyhow!("invalid getBalance response: {}", v))?;

        Ok(lamports)
    }

    /// Returns:
    /// - amount in base units (u128)
    /// - decimals (u32)
    ///
    /// Implementation:
    /// - getTokenAccountsByOwner(owner, {mint}, {encoding:"jsonParsed"})
    /// - sum tokenAmount.amount across all returned token accounts (owner can have multiple)
    /// - decimals taken from tokenAmount.decimals (prefer first seen)
    pub async fn get_spl_balance_base_units(
        &self,
        owner: &str,
        mint: &str
    ) -> Result<(u128, u32), anyhow::Error> {
        let payload =
            json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner,
                { "mint": mint },
                { "encoding": "jsonParsed" }
            ]
        });

        let v = self.rpc_call(payload).await?;

        let arr = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("invalid getTokenAccountsByOwner response: {}", v))?;

        if arr.is_empty() {
            return Ok((0u128, 0u32));
        }

        let mut sum: u128 = 0;
        let mut decimals: u32 = 0;

        for item in arr {
            // item.account.data.parsed.info.tokenAmount.amount / decimals
            let token_amount = item
                .get("account")
                .and_then(|a| a.get("data"))
                .and_then(|d| d.get("parsed"))
                .and_then(|p| p.get("info"))
                .and_then(|i| i.get("tokenAmount"))
                .ok_or_else(|| anyhow!("missing tokenAmount in parsed account: {}", item))?;

            let amt_str = token_amount
                .get("amount")
                .and_then(|x| x.as_str())
                .unwrap_or("0");

            let amt: u128 = amt_str.parse::<u128>().unwrap_or(0);
            sum = sum.saturating_add(amt);

            if decimals == 0 {
                if let Some(d) = token_amount.get("decimals").and_then(|x| x.as_u64()) {
                    decimals = d as u32;
                }
            }
        }

        Ok((sum, decimals))
    }

    async fn rpc_call(&self, payload: Value) -> Result<Value, anyhow::Error> {
        let res = self.http.post(&self.rpc_url).json(&payload).send().await?;

        let status = res.status();
        let v: Value = res.json().await?;

        if !status.is_success() {
            return Err(anyhow!("sol rpc http error: status={} body={}", status, v));
        }

        if let Some(err) = v.get("error") {
            return Err(anyhow!("sol rpc error: {}", err));
        }

        Ok(v)
    }
}
