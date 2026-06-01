use crate::chains::{supported_evm_networks, ChainKey};
use crate::config::AppConfig;
use crate::evm::rpc::RpcClient;
use crate::solana::rpc::SolanaRpcClient;
use crate::tron::multicall3::fetch_single_trc20_decimals;
use crate::tron::rpc::TronRpcClient;
use thiserror::Error;

const EVM_DECIMALS_SELECTOR: &str = "0x313ce567";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenDecimalsChain {
    Evm(ChainKey),
    Solana,
    Tron,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenDecimalsTarget {
    pub blockchain: String,
    pub contract_address: String,
    pub chain: TokenDecimalsChain,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum TokenDecimalsValidationError {
    #[error("unsupported blockchain: {0}")]
    UnsupportedBlockchain(String),

    #[error("invalid contract address for {blockchain}: {contract_address}")]
    InvalidContractAddress {
        blockchain: String,
        contract_address: String,
    },
}

pub fn normalize_token_decimals_target(
    blockchain: &str,
    contract_address: &str,
) -> Result<TokenDecimalsTarget, TokenDecimalsValidationError> {
    let blockchain = normalize_blockchain_key(blockchain);
    let contract_address = contract_address.trim();

    if blockchain.is_empty() {
        return Err(TokenDecimalsValidationError::UnsupportedBlockchain(
            blockchain,
        ));
    }

    if blockchain == "sol" {
        if !is_valid_solana_pubkey_32(contract_address) {
            return Err(TokenDecimalsValidationError::InvalidContractAddress {
                blockchain,
                contract_address: contract_address.to_string(),
            });
        }

        return Ok(TokenDecimalsTarget {
            blockchain,
            contract_address: contract_address.to_string(),
            chain: TokenDecimalsChain::Solana,
        });
    }

    if blockchain == "trx" {
        if !crate::core::normalize::is_valid_tron_address(contract_address) {
            return Err(TokenDecimalsValidationError::InvalidContractAddress {
                blockchain,
                contract_address: contract_address.to_string(),
            });
        }

        return Ok(TokenDecimalsTarget {
            blockchain,
            contract_address: contract_address.to_string(),
            chain: TokenDecimalsChain::Tron,
        });
    }

    let evm_chains = supported_evm_networks();
    let Some(chain) = evm_chains.get(blockchain.as_str()).copied() else {
        return Err(TokenDecimalsValidationError::UnsupportedBlockchain(
            blockchain,
        ));
    };

    let Some(contract_address) = normalize_evm_h160(contract_address) else {
        return Err(TokenDecimalsValidationError::InvalidContractAddress {
            blockchain,
            contract_address: contract_address.to_string(),
        });
    };

    Ok(TokenDecimalsTarget {
        blockchain,
        contract_address,
        chain: TokenDecimalsChain::Evm(chain),
    })
}

pub async fn fetch_token_decimals(
    cfg: &AppConfig,
    target: &TokenDecimalsTarget,
) -> Result<Option<u32>, anyhow::Error> {
    match target.chain {
        TokenDecimalsChain::Evm(chain) => {
            let rpc = RpcClient::new(
                chain.thirdweb_rpc_url(&cfg.thirdweb_client_id),
                cfg.rpc_timeout_ms,
            );
            fetch_evm_token_decimals(&rpc, &target.contract_address).await
        }
        TokenDecimalsChain::Solana => {
            let rpc = SolanaRpcClient::new(cfg.solana_rpc_url.clone(), cfg.rpc_timeout_ms);
            rpc.get_spl_mint_decimals(&target.contract_address).await
        }
        TokenDecimalsChain::Tron => {
            if cfg.tron_fullnode_url.trim().is_empty() && cfg.tron_solidity_url.trim().is_empty() {
                return Err(anyhow::anyhow!("TRON RPC URL is not configured"));
            }

            let rpc = TronRpcClient::new(
                cfg.tron_fullnode_url.clone(),
                cfg.tron_solidity_url.clone(),
                cfg.tron_api_key.clone(),
                cfg.rpc_timeout_ms,
            );
            fetch_single_trc20_decimals(&rpc, &target.contract_address).await
        }
    }
}

async fn fetch_evm_token_decimals(
    rpc: &RpcClient,
    contract_address: &str,
) -> Result<Option<u32>, anyhow::Error> {
    let code = rpc.eth_get_code(contract_address).await?;
    if is_empty_evm_code(&code) {
        return Ok(None);
    }

    let raw = match rpc.eth_call(contract_address, EVM_DECIMALS_SELECTOR).await {
        Ok(raw) => raw,
        Err(e) => {
            tracing::warn!(
                contract = %contract_address,
                error = %e,
                "EVM decimals() call failed; treating token as not found"
            );
            return Ok(None);
        }
    };

    Ok(decode_evm_decimals_return(&raw))
}

fn normalize_blockchain_key(blockchain: &str) -> String {
    let key = blockchain
        .trim()
        .to_lowercase()
        .replace('_', "-")
        .replace(' ', "-");

    match key.as_str() {
        "ethereum" => "eth".to_string(),
        "binance" | "binance-smart-chain" | "bsc" => "bnb".to_string(),
        "polygon" | "polygon-pos" => "matic".to_string(),
        "optimism" => "op".to_string(),
        "arbitrum" | "arbitrum-one" => "arb1".to_string(),
        "avalanche" | "avalanche-c-chain" => "avax".to_string(),
        "fantom" => "ftm".to_string(),
        "cronos" => "cro".to_string(),
        "rootstock" => "rstk".to_string(),
        "ethereum-classic" => "ethc".to_string(),
        "klaytn" | "klaytn-cypress" => "cypress".to_string(),
        "okx" | "okc" | "okx-chain" => "okxchain".to_string(),
        "solana" => "sol".to_string(),
        "tron" => "trx".to_string(),
        _ => key,
    }
}

fn normalize_evm_h160(address: &str) -> Option<String> {
    let trimmed = address.trim();
    let hex = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    if hex.len() != 40 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    Some(format!("0x{}", hex.to_ascii_lowercase()))
}

fn is_valid_solana_pubkey_32(address: &str) -> bool {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return false;
    }

    bs58::decode(trimmed)
        .into_vec()
        .map(|bytes| bytes.len() == 32)
        .unwrap_or(false)
}

fn is_empty_evm_code(code: &str) -> bool {
    let clean = code.trim().strip_prefix("0x").unwrap_or(code.trim());
    clean.is_empty() || clean.chars().all(|c| c == '0')
}

fn decode_evm_decimals_return(raw_hex: &str) -> Option<u32> {
    let clean = raw_hex.trim().strip_prefix("0x").unwrap_or(raw_hex.trim());
    if clean.is_empty() || clean.len() % 2 != 0 {
        return None;
    }

    let bytes = hex::decode(clean).ok()?;
    if bytes.len() < 32 {
        return None;
    }

    let word = &bytes[bytes.len() - 32..];
    if word[..31].iter().any(|&b| b != 0) {
        return None;
    }

    Some(word[31] as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_common_blockchain_aliases() {
        assert_eq!(normalize_blockchain_key("Ethereum"), "eth");
        assert_eq!(normalize_blockchain_key("bsc"), "bnb");
        assert_eq!(normalize_blockchain_key("Solana"), "sol");
        assert_eq!(normalize_blockchain_key("tron"), "trx");
    }

    #[test]
    fn normalizes_evm_contract_address() {
        assert_eq!(
            normalize_evm_h160("52908400098527886E0F7030069857D2E4169EE7").unwrap(),
            "0x52908400098527886e0f7030069857d2e4169ee7"
        );
        assert!(normalize_evm_h160("0x1234").is_none());
    }

    #[test]
    fn validates_target_by_chain_family() {
        let evm =
            normalize_token_decimals_target("bsc", "0x55d398326f99059ff775485246999027b3197955")
                .unwrap();
        assert_eq!(evm.blockchain, "bnb");
        assert!(matches!(evm.chain, TokenDecimalsChain::Evm(ChainKey::Bnb)));

        let invalid = normalize_token_decimals_target("solana", "not-a-mint").unwrap_err();
        assert!(matches!(
            invalid,
            TokenDecimalsValidationError::InvalidContractAddress { .. }
        ));
    }

    #[test]
    fn evm_decimals_decoder_requires_full_abi_word() {
        assert_eq!(
            decode_evm_decimals_return(
                "0x0000000000000000000000000000000000000000000000000000000000000006"
            ),
            Some(6)
        );
        assert_eq!(decode_evm_decimals_return("0x06"), None);

        let mut too_large = [0u8; 32];
        too_large[30] = 1;
        too_large[31] = 1;
        assert_eq!(
            decode_evm_decimals_return(&format!("0x{}", hex::encode(too_large))),
            None
        );
    }

    #[test]
    fn empty_evm_code_is_detected() {
        assert!(is_empty_evm_code("0x"));
        assert!(is_empty_evm_code("0x0000"));
        assert!(!is_empty_evm_code("0x6000"));
    }

    #[test]
    fn normalizes_additional_evm_aliases_and_passthrough_keys() {
        // Passthrough keys that exist in supported_evm_networks() but had no explicit test
        assert_eq!(normalize_blockchain_key("gnosis"), "gnosis");
        assert_eq!(normalize_blockchain_key("base"), "base");
        assert_eq!(normalize_blockchain_key("heco"), "heco");
        assert_eq!(normalize_blockchain_key("callisto"), "callisto");
        assert_eq!(normalize_blockchain_key("linea"), "linea");
        assert_eq!(normalize_blockchain_key("palm"), "palm");
        assert_eq!(normalize_blockchain_key("mantle"), "mantle");
        assert_eq!(normalize_blockchain_key("mint"), "mint");
        assert_eq!(normalize_blockchain_key("iotex"), "iotex");
        assert_eq!(normalize_blockchain_key("aurora"), "aurora");

        // Additional alias paths
        assert_eq!(normalize_blockchain_key("Polygon-Pos"), "matic");
        assert_eq!(normalize_blockchain_key("Arbitrum One"), "arb1");
        assert_eq!(normalize_blockchain_key("Avalanche C-Chain"), "avax");
    }

    // --- Phase 4: Validation error path coverage (the 400 cases) ---

    #[test]
    fn errors_on_empty_or_unsupported_blockchain() {
        let err = normalize_token_decimals_target("", "0x123").unwrap_err();
        assert!(matches!(err, TokenDecimalsValidationError::UnsupportedBlockchain(b) if b.is_empty()));

        let err = normalize_token_decimals_target("   ", "0x123").unwrap_err();
        assert!(matches!(err, TokenDecimalsValidationError::UnsupportedBlockchain(b) if b.is_empty()));

        let err = normalize_token_decimals_target("foo", "0x123").unwrap_err();
        assert!(matches!(err, TokenDecimalsValidationError::UnsupportedBlockchain(b) if b == "foo"));

        let err = normalize_token_decimals_target("unknownchain", "0xabc").unwrap_err();
        assert!(matches!(err, TokenDecimalsValidationError::UnsupportedBlockchain(b) if b == "unknownchain"));
    }

    #[test]
    fn errors_on_invalid_evm_contract_addresses() {
        let bad_cases = vec![
            "0x123".to_string(),                    // too short
            "0x".to_string() + &"g".repeat(40),     // invalid hex
            "1".repeat(39),                         // wrong length
            "0x".to_string() + &"0".repeat(39),     // 39 hex chars
            "".to_string(),                         // empty
        ];

        for addr in bad_cases {
            let err = normalize_token_decimals_target("eth", &addr).unwrap_err();
            assert!(
                matches!(err, TokenDecimalsValidationError::InvalidContractAddress { blockchain, contract_address } if blockchain == "eth" && contract_address == addr),
                "failed for address: {addr}"
            );
        }
    }

    #[test]
    fn errors_on_invalid_solana_mints() {
        // Not valid base58 or wrong decoded length (our validator requires exactly 32 bytes)
        let bad_cases = [
            "not-base58!",
            "1111111111111111111111111111111", // decodes to 31 bytes
            "TooLongForASolanaAddressThatShouldBeRejectedBecauseItExceeds32Bytes",
            "",
        ];

        for addr in bad_cases {
            let err = normalize_token_decimals_target("sol", addr).unwrap_err();
            assert!(
                matches!(err, TokenDecimalsValidationError::InvalidContractAddress { blockchain, .. } if blockchain == "sol"),
                "failed for solana address: {addr}"
            );
        }
    }

    #[test]
    fn errors_on_invalid_tron_addresses() {
        let bad_cases = [
            "0x1234567890123456789012345678901234567890", // EVM style
            "T1234",                                      // too short
            "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6tX",        // bad checksum (extra char)
            "410000000000000000000000000000000000000000", // hex instead of base58check
            "",
        ];

        for addr in bad_cases {
            let err = normalize_token_decimals_target("trx", addr).unwrap_err();
            assert!(
                matches!(err, TokenDecimalsValidationError::InvalidContractAddress { blockchain, .. } if blockchain == "trx"),
                "failed for tron address: {addr}"
            );
        }
    }

    // --- Phase 5: Live E2E RPC tests (real network calls when env vars present) ---

    fn build_minimal_live_config() -> AppConfig {
        // Load the real .env from the main project (outside this worktree)
        let _ = dotenvy::from_path("D:/Learn/rust/balance-service/.env");

        AppConfig {
            bind_addr: "[::]:0".to_string(),
            mongodb_uri: "mongodb://localhost:27017/dummy".to_string(),
            mongodb_db_main: "dummy".to_string(),
            worker_enabled: false,
            worker_poll_ms: 1000,
            worker_concurrency: 1,
            worker_slow_ms: 0,
            nats_url: "nats://127.0.0.1:4222".to_string(),
            nats_stream: "DUMMY".to_string(),
            nats_subject: "dummy".to_string(),
            nats_durable: "dummy".to_string(),
            nats_max_ack_pending: 1,
            nats_ack_wait_secs: 10,
            thirdweb_client_id: std::env::var("THIRD_WEB_CLIENT_ID")
                .expect("THIRD_WEB_CLIENT_ID must be set for live E2E tests"),
            solana_rpc_url: std::env::var("SOLANA_RPC_URL")
                .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string()),
            rpc_timeout_ms: 20_000,
            tron_fullnode_url: std::env::var("TRON_FULLNODE_URL").unwrap_or_default(),
            tron_solidity_url: std::env::var("TRON_SOLIDITY_URL").unwrap_or_default(),
            tron_api_key: std::env::var("TRON_API_KEY")
                .ok()
                .or_else(|| std::env::var("TRON_TEMP_KEY").ok()),
        }
    }

    #[tokio::test]
    #[ignore = "requires THIRD_WEB_CLIENT_ID + network; run with `cargo test -- --ignored` when env is set"]
    async fn live_e2e_eth_usdt_decimals() {
        let cfg = build_minimal_live_config();
        let target = normalize_token_decimals_target("eth", "0xdAC17F958D2ee523a2206206994597C13D831ec7").unwrap();
        let dec = fetch_token_decimals(&cfg, &target).await.expect("ETH USDT live call failed");
        assert_eq!(dec, Some(6), "ETH USDT should be 6 decimals");
    }

    #[tokio::test]
    #[ignore = "requires THIRD_WEB_CLIENT_ID + network; run with `cargo test -- --ignored` when env is set"]
    async fn live_e2e_bnb_usdt_decimals() {
        let cfg = build_minimal_live_config();
        let target = normalize_token_decimals_target("bnb", "0x55d398326f99059ff775485246999027b3197955").unwrap();
        let dec = fetch_token_decimals(&cfg, &target).await.expect("BNB USDT live call failed");
        assert_eq!(dec, Some(18), "BNB USDT should be 18 decimals");
    }

    #[tokio::test]
    #[ignore = "requires THIRD_WEB_CLIENT_ID + network; run with `cargo test -- --ignored` when env is set"]
    async fn live_e2e_sol_usdc_decimals() {
        let cfg = build_minimal_live_config();
        let target = normalize_token_decimals_target("sol", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();
        let dec = fetch_token_decimals(&cfg, &target).await.expect("Solana USDC live call failed");
        assert_eq!(dec, Some(6), "Solana USDC should be 6 decimals");
    }

    #[tokio::test]
    #[ignore = "requires THIRD_WEB_CLIENT_ID + network; run with `cargo test -- --ignored` when env is set"]
    async fn live_e2e_tron_usdt_decimals() {
        let cfg = build_minimal_live_config();
        if cfg.tron_fullnode_url.is_empty() && cfg.tron_solidity_url.is_empty() {
            eprintln!("Skipping Tron live test - no TRON_*_URL set");
            return;
        }
        let target = normalize_token_decimals_target("trx", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").unwrap();
        let dec = fetch_token_decimals(&cfg, &target).await.expect("Tron USDT live call failed");
        assert_eq!(dec, Some(6), "Tron USDT (TRC20) should be 6 decimals");
    }

    #[tokio::test]
    #[ignore = "requires THIRD_WEB_CLIENT_ID + network; run with `cargo test -- --ignored` when env is set"]
    async fn live_e2e_nonexistent_evm_contract() {
        let cfg = build_minimal_live_config();
        let target = normalize_token_decimals_target("eth", "0x0000000000000000000000000000000000000001").unwrap();
        let dec = fetch_token_decimals(&cfg, &target).await.expect("EVM nonexistent call failed");
        assert_eq!(dec, None, "Random EOA / no-code address should return None (exists=false)");
    }
}
