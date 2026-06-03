use anyhow::anyhow;
use base64::{engine::general_purpose, Engine as _};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Clone)]
pub struct TronRpcClient {
    http: Client,
    fullnode_url: String,
    solidity_url: String,
    api_key: Option<String>,
}

impl TronRpcClient {
    pub fn new(
        fullnode_url: String,
        solidity_url: String,
        api_key: Option<String>,
        timeout_ms: u64,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .user_agent("balance-service/1.0 (tron)")
            .build()
            .expect("failed to build reqwest client");

        Self {
            http,
            fullnode_url: fullnode_url.trim_end_matches('/').to_string(),
            solidity_url: solidity_url.trim_end_matches('/').to_string(),
            api_key,
        }
    }

    fn post(&self, url: &str, payload: &Value) -> reqwest::RequestBuilder {
        let mut rb = self.http.post(url).json(payload);
        if let Some(k) = &self.api_key {
            rb = rb.header("TRON-PRO-API-KEY", k);
        }
        rb
    }

    async fn post_json(&self, url: &str, payload: &Value) -> Result<Value, anyhow::Error> {
        let res = self.post(url, payload).send().await?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!(
                "TRON HTTP error {} @ {} | body={}",
                status,
                url,
                text.chars().take(600).collect::<String>()
            ));
        }

        let v: Value = serde_json::from_str(&text).map_err(|e| {
            anyhow!(
                "TRON JSON decode failed @ {}: {} | body={}",
                url,
                e,
                text.chars().take(600).collect::<String>()
            )
        })?;

        Ok(v)
    }

    fn decode_constant_result(raw: &str) -> Result<Vec<u8>, anyhow::Error> {
        let s = raw.trim();
        if s.is_empty() {
            return Ok(vec![]);
        }

        let clean = s.strip_prefix("0x").unwrap_or(s);
        if !clean.is_empty() && clean.len() % 2 == 0 && clean.chars().all(|c| c.is_ascii_hexdigit())
        {
            if let Ok(bytes) = hex::decode(clean) {
                return Ok(bytes);
            }
        }

        general_purpose::STANDARD.decode(s).map_err(|e| {
            anyhow!(
                "constant_result is neither valid hex nor base64: '{}...' err={}",
                s.chars().take(40).collect::<String>(),
                e
            )
        })
    }

    pub async fn trigger_constant(
        &self,
        contract_b58: &str,
        owner_b58: &str,
        data_hex: &str,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let (base_url, use_solidity) = if !self.solidity_url.is_empty() {
            (&self.solidity_url, true)
        } else {
            (&self.fullnode_url, false)
        };
        let path_segment = if use_solidity {
            "walletsolidity"
        } else {
            "wallet"
        };
        let url = format!("{}/{}/triggerconstantcontract", base_url, path_segment);

        let payload = json!({
            "owner_address": owner_b58,
            "contract_address": contract_b58,
            "data": data_hex,
            "visible": true
        });

        let v = self.post_json(&url, &payload).await?;

        // Many TRON full nodes return a valid response WITHOUT result.result == true.
        // The reliable success signal is: constant_result is present and non-empty.
        // Only hard-fail when result.result is *explicitly* false AND constant_result
        // is absent — that combination is a genuine contract failure or bad request.
        let explicit_fail = v
            .get("result")
            .and_then(|x| x.get("result"))
            .and_then(|x| x.as_bool())
            == Some(false);

        let raw_opt = v
            .get("constant_result")
            .and_then(|x| x.as_array())
            .and_then(|a| a.first())
            .and_then(|x| x.as_str());

        if explicit_fail && raw_opt.is_none() {
            return Err(anyhow!(
                "TRON triggerconstantcontract result=false and no constant_result | body={}",
                v
            ));
        }

        let raw =
            raw_opt.ok_or_else(|| anyhow!("TRON constant_result missing or empty | body={}", v))?;

        Self::decode_constant_result(raw)
    }

    pub async fn getaccount_balance_sun(&self, wallet_b58: &str) -> Result<u64, anyhow::Error> {
        let url = format!("{}/wallet/getaccount", self.fullnode_url);
        let payload = json!({ "address": wallet_b58, "visible": true });
        let v = self.post_json(&url, &payload).await?;
        Ok(v.get("balance").and_then(|x| x.as_u64()).unwrap_or(0))
    }
}
