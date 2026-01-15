// ==================================================
// balance-service\src\tron\rpc.rs
// ==================================================

use anyhow::anyhow;
use ethabi::{Function, Param, ParamType, StateMutability, Token};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use base64::{engine::general_purpose, Engine as _};

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

    // ---------------- low-level HTTP ----------------

    fn post(&self, url: &str, payload: &Value) -> reqwest::RequestBuilder {
        let mut rb = self.http.post(url).json(payload);
        if let Some(k) = &self.api_key {
            // TRON gateway header used by many providers (incl. TronGrid style)
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

    // ---------------- ABI helpers ----------------

    #[allow(deprecated)]
    fn fn_decimals() -> Function {
        Function {
            name: "decimals".to_string(),
            inputs: vec![],
            outputs: vec![Param {
                name: "decimals".to_string(),
                kind: ParamType::Uint(8),
                internal_type: None,
            }],
            constant: None,
            state_mutability: StateMutability::View,
        }
    }

    #[allow(deprecated)]
    fn fn_balance_of() -> Function {
        Function {
            name: "balanceOf".to_string(),
            inputs: vec![Param {
                name: "account".to_string(),
                kind: ParamType::Address,
                internal_type: None,
            }],
            outputs: vec![Param {
                name: "balance".to_string(),
                kind: ParamType::Uint(256),
                internal_type: None,
            }],
            constant: None,
            state_mutability: StateMutability::View,
        }
    }

    // ---------------- address utils ----------------

    /// Decode TRON base58check (T...) -> hex payload "41" + 20 bytes (21 bytes total) as lowercase hex string.
    pub fn tron_base58_to_hex41(addr_b58: &str) -> Result<String, anyhow::Error> {
        use sha2::{Digest, Sha256};

        let t = addr_b58.trim();
        if t.is_empty() {
            return Err(anyhow!("empty tron address"));
        }

        let decoded = bs58::decode(t).into_vec()?;
        if decoded.len() != 25 {
            return Err(anyhow!("invalid tron base58 length: {}", decoded.len()));
        }

        let (payload, checksum) = decoded.split_at(21);

        // checksum = first 4 bytes of sha256d(payload)
        let h1 = Sha256::digest(payload);
        let h2 = Sha256::digest(&h1);

        if checksum != &h2[..4] {
            return Err(anyhow!("invalid tron checksum"));
        }

        // payload[0] is 0x41 for mainnet
        Ok(hex::encode(payload))
    }

    /// Convert EVM 0x + 20 bytes to TRON base58check address (starts with T)
    pub fn evm_hex_to_tron_base58(evm: &str) -> Result<String, anyhow::Error> {
        use sha2::{Digest, Sha256};

        let hex20 = evm.trim().strip_prefix("0x").unwrap_or(evm.trim());
        if hex20.len() != 40 {
            return Err(anyhow!(
                "invalid evm address len (expected 40 hex chars): {}",
                hex20.len()
            ));
        }

        let addr20 = hex::decode(hex20)?;
        if addr20.len() != 20 {
            return Err(anyhow!("invalid evm address bytes len: {}", addr20.len()));
        }

        // TRON payload = 0x41 + 20 bytes
        let mut payload = Vec::with_capacity(21);
        payload.push(0x41);
        payload.extend_from_slice(&addr20);

        // checksum
        let h1 = Sha256::digest(&payload);
        let h2 = Sha256::digest(&h1);

        let mut out = Vec::with_capacity(25);
        out.extend_from_slice(&payload);
        out.extend_from_slice(&h2[..4]);

        Ok(bs58::encode(out).into_string())
    }

    // ---------------- constant_result decode ----------------

    /// Decodes constant_result which can be:
    /// 1. Hex string (standard)
    /// 2. Base64 string (TronGrid / some GetBlock nodes)
    fn decode_constant_result(raw: &str) -> Result<Vec<u8>, anyhow::Error> {
        let s = raw.trim();
        if s.is_empty() {
            return Ok(vec![]);
        }

        // 1. Try Hex first
        let clean_hex = s.strip_prefix("0x").unwrap_or(s);
        // Basic filter: if it contains non-hex chars, it's definitely not hex (or it's broken hex)
        if clean_hex.chars().all(|c| c.is_ascii_hexdigit()) {
            // Handle odd-length hex if necessary
            let hex_input = if clean_hex.len() % 2 != 0 {
                format!("0{}", clean_hex)
            } else {
                clean_hex.to_string()
            };
            
            if let Ok(bytes) = hex::decode(&hex_input) {
                return Ok(bytes);
            }
        }

        // 2. Fallback to Base64
        match general_purpose::STANDARD.decode(s) {
            Ok(bytes) => {
                tracing::debug!(len = bytes.len(), "decoded constant_result via Base64");
                Ok(bytes)
            },
            Err(_) => {
                // Return generic error if both fail
                Err(anyhow!("constant_result is neither valid Hex nor Base64: {}", s))
            }
        }
    }

    fn extract_constant_result(v: &Value) -> Result<Vec<u8>, anyhow::Error> {
        // Typical response:
        // { "result": { "result": true }, "constant_result": ["..."], ... }
        // If revert/blocked: constant_result may be empty or missing.
        let ok = v
            .get("result")
            .and_then(|r| r.get("result"))
            .and_then(|x| x.as_bool());

        if ok == Some(false) {
            // some providers include message/code fields
            return Ok(vec![]);
        }

        // Make it a slice so we can safely default with &[]
        let arr: &[Value] = v
            .get("constant_result")
            .and_then(|x| x.as_array())
            .map(|a| a.as_slice())
            .unwrap_or(&[]);

        if arr.is_empty() {
            return Ok(vec![]);
        }

        let raw0 = arr[0]
            .as_str()
            .ok_or_else(|| anyhow!("constant_result[0] not string: {}", v))?;

        Self::decode_constant_result(raw0)
    }

    // ---------------- core TRON call ----------------

    async fn trigger_constant(
        &self,
        contract_b58: &str,
        owner_b58: &str,
        function_selector: &str,
        parameter_hex: &str,
    ) -> Result<Vec<u8>, anyhow::Error> {
        // Determine correct endpoint based on configured URLs.
        // If a solidity URL is provided, we MUST use `/walletsolidity/`.
        // If only fullnode URL is provided, we use `/wallet/`.
        let (base_url, use_solidity) = if !self.solidity_url.is_empty() {
            (&self.solidity_url, true)
        } else {
            (&self.fullnode_url, false)
        };

        // Fix: Use correct path segment to avoid 405 Method Not Allowed
        let path_segment = if use_solidity { "walletsolidity" } else { "wallet" };
        let url = format!("{}/{}/triggerconstantcontract", base_url, path_segment);

        let payload = json!({
            "owner_address": owner_b58,
            "contract_address": contract_b58,
            "function_selector": function_selector,
            "parameter": parameter_hex, // hex-encoded ABI args ONLY, no 0x
            "visible": true
        });

        tracing::debug!(
            url = %url,
            function = %function_selector,
            contract = %contract_b58,
            owner = %owner_b58,
            param_len = parameter_hex.len(),
            "TRON triggerconstantcontract → sending"
        );

        let v = self.post_json(&url, &payload).await?;

        // TRON can still return 200 with { result: { result:false }, ... }
        // We treat that as empty (caller falls back to zero/default).
        let out = Self::extract_constant_result(&v)?;

        tracing::debug!(
            function = %function_selector,
            contract = %contract_b58,
            out_len = out.len(),
            "TRON triggerconstantcontract ← parsed"
        );

        Ok(out)
    }

    // ---------------- public API ----------------

    /// Native TRX balance in SUN (1 TRX = 1_000_000 SUN)
    pub async fn get_trx_balance_sun(&self, wallet_b58: &str) -> Result<u64, anyhow::Error> {
        let url = format!("{}/wallet/getaccount", self.fullnode_url);
        let payload = json!({ "address": wallet_b58, "visible": true });

        tracing::debug!(wallet=%wallet_b58, "TRON getaccount → sending");

        let v = self.post_json(&url, &payload).await?;

        // If account doesn't exist yet, getaccount may return {} or missing fields.
        let bal = v.get("balance").and_then(|x| x.as_u64()).unwrap_or(0);

        tracing::debug!(wallet=%wallet_b58, balance_sun=bal, "TRON getaccount ← parsed");

        Ok(bal)
    }

    pub async fn get_trc20_decimals(
        &self,
        contract_b58: &str,
        owner_b58: &str,
    ) -> Result<u32, anyhow::Error> {
        // NOTE: Some nodes require owner to be a real account; but most accept any valid address.
        let f = Self::fn_decimals();
        let out = self
            .trigger_constant(contract_b58, owner_b58, "decimals()", "")
            .await?;

        if out.is_empty() {
            // safest fallback (most ERC20s / TRC20s are 18)
            return Ok(18);
        }

        let decoded = f.decode_output(&out)?;
        let dec = decoded.get(0).and_then(|t| match t {
            Token::Uint(u) => Some(u.as_u32()),
            _ => None,
        });

        Ok(dec.unwrap_or(18))
    }

    /// TRC20 balance in base units (raw integer)
    pub async fn get_trc20_balance(
        &self,
        contract_b58: &str,
        owner_b58: &str,
    ) -> Result<u128, anyhow::Error> {
        // Keep ABI definition for decoding the OUTPUT only
        let f = Self::fn_balance_of();

        // ---------------------------------------------------------
        // FIX: Manually encode input to include the 0x41 TRON byte.
        // ethabi::Token::Address forces 20 bytes, stripping 0x41.
        // We need: [11 bytes padding] + [0x41] + [20 bytes address]
        // ---------------------------------------------------------
        
        // 1. Get the 21-byte hex string (e.g., "41..." + 40 chars)
        let addr_hex_41 = Self::tron_base58_to_hex41(owner_b58)?;

        // 2. Pad to 32 bytes (64 hex characters). 
        //    21 bytes = 42 chars. 
        //    32 bytes = 64 chars.
        //    64 - 42 = 22 characters of zero padding needed.
        let param_hex = format!("{}{}", "0".repeat(22), addr_hex_41);

        let out = self
            .trigger_constant(contract_b58, owner_b58, "balanceOf(address)", &param_hex)
            .await?;

        if out.is_empty() {
            return Ok(0);
        }

        let decoded = f.decode_output(&out)?;
        let u = match decoded.get(0) {
            Some(Token::Uint(v)) => v.clone(),
            _ => {
                return Err(anyhow!("invalid TRC20 balanceOf decode"));
            }
        };

        // Convert U256 -> u128 safely
        let mut buf = [0u8; 32];
        u.to_big_endian(&mut buf);
        Ok(u128::from_be_bytes(
            buf[16..].try_into().expect("slice len checked"),
        ))
    }
}