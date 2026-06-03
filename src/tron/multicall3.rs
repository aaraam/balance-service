// src/tron/multicall3.rs
//
// TRON balance fetching layer.
// TRON has no Multicall3. Calls are made individually via TronRpcClient::trigger_constant
// and TronRpcClient::getaccount_balance_sun. Concurrency is managed with buffer_unordered
// over bounded pair-chunks to avoid provider rate-limit bursts.

use ethereum_types::U256;
use futures::stream::{self, StreamExt};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::rpc::TronRpcClient;

// ── public constants ──────────────────────────────────────────────────────────
// These are exported so runner.rs can import them by name without hard-coding.
pub const TRON_NATIVE_CONCURRENCY: usize = 6;
pub const TRON_TRC20_CONCURRENCY: usize = 6;
pub const TRON_DECIMALS_CONCURRENCY: usize = 3;
pub const TRON_PAIR_CHUNK_SIZE: usize = 75;
pub const TRON_DECIMALS_CHUNK_SIZE: usize = 20;
const TRON_MAX_SUPPORTED_DECIMALS: u32 = 77;

// Kept for backwards-compatibility with runner.rs import list.
// The value is now only used as a fallback; functions use their own defaults above.
pub const MAX_TRON_CALLS_PER_BATCH: usize = TRON_PAIR_CHUNK_SIZE;

// ── ABI selectors ─────────────────────────────────────────────────────────────
// keccak256("balanceOf(address)")[..4]
const SELECTOR_BALANCE_OF: [u8; 4] = [0x70, 0xa0, 0x82, 0x31];
// keccak256("decimals()")[..4]
const SELECTOR_DECIMALS: [u8; 4] = [0x31, 0x3c, 0xe5, 0x67];

// ── helpers ───────────────────────────────────────────────────────────────────

/// Decode a base58check TRON address (T…, 25 bytes on wire) into its inner 20-byte payload.
/// Layout: 0x41 (1 byte) | 20-byte address | 4-byte checksum.
fn tron_b58_to_20bytes(b58: &str) -> Option<[u8; 20]> {
    let decoded = bs58::decode(b58).into_vec().ok()?;
    if decoded.len() != 25 {
        return None;
    }
    if decoded[0] != 0x41 {
        return None;
    }

    let payload = &decoded[..21];
    let checksum = &decoded[21..25];
    let h1 = Sha256::digest(payload);
    let h2 = Sha256::digest(h1);
    if &h2[..4] != checksum {
        return None;
    }

    let mut out = [0u8; 20];
    out.copy_from_slice(&decoded[1..21]);
    Some(out)
}

/// Encode a `balanceOf(address)` ABI call with `address_20` left-padded to 32 bytes.
fn encode_balance_of(address_20: &[u8; 20]) -> String {
    let mut data = Vec::with_capacity(36);
    data.extend_from_slice(&SELECTOR_BALANCE_OF);
    data.extend_from_slice(&[0u8; 12]); // ABI: left-pad address to 32 bytes
    data.extend_from_slice(address_20);
    hex::encode(data)
}

/// Encode a `decimals()` ABI call (no arguments).
fn encode_decimals() -> String {
    hex::encode(SELECTOR_DECIMALS)
}

/// Decode an ABI uint256 return value without truncating large balances.
fn decode_u256_from_returndata(bytes: &[u8]) -> U256 {
    if bytes.len() >= 32 {
        return U256::from_big_endian(&bytes[bytes.len() - 32..]);
    }

    let mut padded = [0u8; 32];
    padded[32 - bytes.len()..].copy_from_slice(bytes);
    U256::from_big_endian(&padded)
}

/// Decode a usable token decimals value for the output formatter.
fn decode_decimals_from_returndata(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 32 {
        return None;
    }
    let start = bytes.len().saturating_sub(32);
    let word = &bytes[start..start + 32];

    if word[..31].iter().any(|&b| b != 0) {
        tracing::warn!("TRON decimals() returned out-of-range value");
        return None;
    }

    let decimals = word[31] as u32;
    if decimals > TRON_MAX_SUPPORTED_DECIMALS {
        tracing::warn!(decimals, "TRON decimals() exceeds formatter range");
        return None;
    }

    Some(decimals)
}

// ── public API ────────────────────────────────────────────────────────────────

/// Fetch native TRX balances (in Sun) for a list of base58 TRON wallets, concurrently.
///
/// `_concurrency` is accepted for API compatibility with existing call sites in runner.rs
/// but the internal constant `TRON_NATIVE_CONCURRENCY` is used as the effective cap.
///
/// Per-wallet errors are swallowed: the wallet gets a balance of 0.
/// Returns: `HashMap<b58_wallet, sun_balance>`
pub async fn fetch_native_trx_concurrent(
    rpc: &TronRpcClient,
    b58_wallets: &[String],
    _concurrency: usize, // kept for API compat; internal constant is used
) -> HashMap<String, u64> {
    let pairs: Vec<(String, u64)> = stream::iter(b58_wallets.iter().cloned())
        .map(|wallet| {
            let rpc = rpc.clone();
            async move {
                let sun = rpc
                    .getaccount_balance_sun(&wallet)
                    .await
                    .unwrap_or_else(|e| {
                        tracing::warn!(
                            wallet = %wallet,
                            error = %e,
                            "TRON native balance fetch failed -> 0"
                        );
                        0
                    });
                (wallet, sun)
            }
        })
        .buffer_unordered(TRON_NATIVE_CONCURRENCY)
        .collect()
        .await;

    pairs.into_iter().collect()
}

/// Fetch TRC20 `balanceOf` for every (wallet, contract) pair, processed in chunks
/// of `TRON_PAIR_CHUNK_SIZE` with `TRON_TRC20_CONCURRENCY` concurrent calls per chunk.
///
/// `_concurrency` is accepted for API compatibility but ignored; internal constants govern
/// actual behaviour.
///
/// Per-call errors are swallowed: the (wallet, contract) entry gets 0.
/// Returns: `HashMap<b58_wallet, HashMap<contract_b58, amount_u256>>`
pub async fn fetch_all_tron_balances(
    rpc: &TronRpcClient,
    b58_wallets: &[String],
    trc20_contracts: &[String],
    _concurrency: usize, // kept for API compat
) -> HashMap<String, HashMap<String, U256>> {
    // Flatten all (wallet, contract) pairs
    let pairs: Vec<(String, String)> = b58_wallets
        .iter()
        .flat_map(|w| trc20_contracts.iter().map(move |c| (w.clone(), c.clone())))
        .collect();

    let mut out: HashMap<String, HashMap<String, U256>> = HashMap::new();

    // Process in chunks to avoid provider bursts
    for chunk in pairs.chunks(TRON_PAIR_CHUNK_SIZE) {
        let chunk_results: Vec<(String, String, U256)> = stream::iter(chunk.iter().cloned())
            .map(|(wallet, contract)| {
                let rpc = rpc.clone();
                async move {
                    let amount = match tron_b58_to_20bytes(&wallet) {
                        Some(addr_bytes) => {
                            let data_hex = encode_balance_of(&addr_bytes);
                            rpc.trigger_constant(&contract, &wallet, &data_hex)
                                .await
                                .map(|bytes: Vec<u8>| decode_u256_from_returndata(&bytes))
                                .unwrap_or_else(|e| {
                                    tracing::warn!(
                                        wallet = %wallet,
                                        contract = %contract,
                                        error = %e,
                                        "TRON TRC20 balanceOf failed -> 0"
                                    );
                                    U256::zero()
                                })
                        }
                        None => {
                            tracing::warn!(
                                wallet = %wallet,
                                "TRON b58 decode failed in fetch_all_tron_balances -> 0"
                            );
                            U256::zero()
                        }
                    };
                    (wallet, contract, amount)
                }
            })
            .buffer_unordered(TRON_TRC20_CONCURRENCY)
            .collect()
            .await;

        for (wallet, contract, amount) in chunk_results {
            out.entry(wallet).or_default().insert(contract, amount);
        }
    }

    out
}

/// Fetch `decimals()` for a list of TRC20 contracts, using `owner_b58` as the caller.
/// Contracts are processed in chunks of `TRON_DECIMALS_CHUNK_SIZE` with
/// `TRON_DECIMALS_CONCURRENCY` concurrent calls per chunk.
///
/// Per-contract errors are swallowed: no decimals entry is returned for that contract.
/// Returns: `HashMap<contract_b58, decimals_u32>`
pub async fn fetch_trc20_decimals(
    rpc: &TronRpcClient,
    trc20_contracts: &[String],
    owner_b58: &str,
    _concurrency: usize, // kept for API compat
) -> HashMap<String, u32> {
    let call_data = encode_decimals();
    let owner = owner_b58.to_string();

    let mut out: HashMap<String, u32> = HashMap::new();

    for chunk in trc20_contracts.chunks(TRON_DECIMALS_CHUNK_SIZE) {
        let chunk_results: Vec<(String, Option<u32>)> = stream::iter(chunk.iter().cloned())
            .map(|contract| {
                let rpc = rpc.clone();
                let owner = owner.clone();
                let data = call_data.clone();
                async move {
                    let dec = rpc
                        .trigger_constant(&contract, &owner, &data)
                        .await
                        .map(|bytes: Vec<u8>| decode_decimals_from_returndata(&bytes))
                        .unwrap_or_else(|e| {
                            tracing::warn!(
                                contract = %contract,
                                error = %e,
                                "TRON decimals() failed; leaving token value unchanged"
                            );
                            None
                        });
                    (contract, dec)
                }
            })
            .buffer_unordered(TRON_DECIMALS_CONCURRENCY)
            .collect()
            .await;

        for (contract, decimals) in chunk_results {
            if let Some(decimals) = decimals {
                out.insert(contract, decimals);
            }
        }
    }

    out
}

/// Fetch `decimals()` for one TRC20 contract.
///
/// `triggerconstantcontract` requires an owner address even for view calls. The
/// contract address is itself a valid TRON address, so it is used as the caller.
/// A failed call means the address could not be proven to expose TRC20 decimals.
pub async fn fetch_single_trc20_decimals(
    rpc: &TronRpcClient,
    contract_b58: &str,
) -> Result<Option<u32>, anyhow::Error> {
    let data = encode_decimals();

    match rpc
        .trigger_constant(contract_b58, contract_b58, &data)
        .await
    {
        Ok(bytes) => Ok(decode_decimals_from_returndata(&bytes)),
        Err(e) => {
            tracing::warn!(
                contract = %contract_b58,
                error = %e,
                "TRON decimals() failed; treating token as not found"
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tron_address_decoder_checks_checksum() {
        let payload = [0x11u8; 20];
        let mut address = vec![0x41];
        address.extend_from_slice(&payload);
        let first_hash = Sha256::digest(&address);
        let second_hash = Sha256::digest(first_hash);
        address.extend_from_slice(&second_hash[..4]);

        let encoded = bs58::encode(&address).into_string();
        assert_eq!(tron_b58_to_20bytes(&encoded), Some(payload));

        address[24] ^= 1;
        let invalid = bs58::encode(address).into_string();
        assert_eq!(tron_b58_to_20bytes(&invalid), None);
    }

    #[test]
    fn balance_decoder_preserves_full_uint256() {
        let raw = [0xff; 32];
        assert_eq!(decode_u256_from_returndata(&raw), U256::MAX);
    }

    #[test]
    fn decimals_decoder_rejects_unsupported_values() {
        let mut raw = [0u8; 32];
        raw[31] = 18;
        assert_eq!(decode_decimals_from_returndata(&raw), Some(18));

        raw[31] = 78;
        assert_eq!(decode_decimals_from_returndata(&raw), None);
    }
}
