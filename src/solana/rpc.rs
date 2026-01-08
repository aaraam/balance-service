use anyhow::anyhow;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

#[derive(Clone)]
pub struct SolanaRpcClient {
    http: Client,
    rpc_url: String,
}

impl SolanaRpcClient {
    pub fn new(rpc_url: String, timeout_ms: u64) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .expect("failed to build reqwest client");

        Self { http, rpc_url }
    }

    pub async fn get_balance_lamports(&self, pubkey: &str) -> Result<u64, anyhow::Error> {
        let payload =
            json!({
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

    /// Fetch SPL token balance for (owner, mint) without computing ATA:
    /// Uses `getTokenAccountsByOwner` with mint filter and jsonParsed encoding.
    ///
    /// Returns: (amount_base_units_u128, decimals_u32)
    pub async fn get_spl_balance_by_owner_mint(
        &self,
        owner_pubkey: &str,
        mint_pubkey: &str
    ) -> Result<(u128, u32), anyhow::Error> {
        let payload =
            json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner_pubkey,
                { "mint": mint_pubkey },
                { "encoding": "jsonParsed" }
            ]
        });

        let res = self.http.post(&self.rpc_url).json(&payload).send().await?;
        let status = res.status();
        let v: serde_json::Value = res.json().await?;

        if !status.is_success() {
            return Err(
                anyhow!(
                    "sol rpc http error (getTokenAccountsByOwner): status={} body={}",
                    status,
                    v
                )
            );
        }
        if let Some(err) = v.get("error") {
            return Err(anyhow!("sol rpc error (getTokenAccountsByOwner): {}", err));
        }

        let arr = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("missing sol result.value array: {}", v))?;

        if arr.is_empty() {
            // token account not found => 0
            return Ok((0u128, 0u32)); // decimals unknown; caller should handle
        }

        // Take first token account (usually ATA, but could be multiple)
        let token_amount = arr[0]
            .get("account")
            .and_then(|a| a.get("data"))
            .and_then(|d| d.get("parsed"))
            .and_then(|p| p.get("info"))
            .and_then(|i| i.get("tokenAmount"))
            .ok_or_else(|| anyhow!("missing parsed.info.tokenAmount: {}", v))?;

        let amount_str = token_amount
            .get("amount")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("missing tokenAmount.amount: {}", v))?;

        let decimals = token_amount
            .get("decimals")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| anyhow!("missing tokenAmount.decimals: {}", v))? as u32;

        let amount_u128: u128 = amount_str
            .parse::<u128>()
            .map_err(|e| anyhow!("invalid token amount '{}': {}", amount_str, e))?;

        Ok((amount_u128, decimals))
    }
}
