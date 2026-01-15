// ==================================================
// balance-service\src\http\validate.rs
// ==================================================

use crate::chains::supported_evm_networks;
use crate::http::dto::{BalanceRequest, ContractGroup};
use crate::http::error::ApiError;
use serde_json::json;

/// Hard limits (anti-abuse / sanity)
const MAX_EVM_WALLETS: usize = 50;
const MAX_SOL_WALLETS: usize = 50;
const MAX_TRON_WALLETS: usize = 50;
const MAX_CONTRACT_GROUPS: usize = 20;
const MAX_CONTRACTS_PER_GROUP: usize = 200;

/// Work-unit caps (prevent combinatorial explosions)
/// EVM work ~= wallets * total_erc20_contracts_requested
const MAX_EVM_WORK_UNITS: usize = 10_000;
/// SOL work ~= sol_wallets * total_mints_requested
const MAX_SOL_WORK_UNITS: usize = 10_000;
/// TRX work ~= tron_wallets * total_trc20_contracts_requested
const MAX_TRON_WORK_UNITS: usize = 10_000;

fn is_valid_tron_base58_addr(s: &str) -> bool {
    use sha2::{Digest, Sha256};

    let t = s.trim();
    if t.is_empty() || !t.starts_with('T') {
        return false;
    }

    let decoded = match bs58::decode(t).into_vec() {
        Ok(v) => v,
        Err(_) => {
            return false;
        }
    };

    if decoded.len() != 25 {
        return false;
    }

    let (payload, checksum) = decoded.split_at(21);

    if payload.first().copied() != Some(0x41) {
        return false;
    }

    let h1 = Sha256::digest(payload);
    let h2 = Sha256::digest(&h1);
    checksum == &h2[..4]
}

/// Validate raw request shape + limits first (before normalize)
pub fn validate_request_limits(req: &BalanceRequest) -> Result<(), ApiError> {
    if req.wallet_addresses.len() > MAX_EVM_WALLETS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!("Too many EVM wallets (max {})", MAX_EVM_WALLETS),
            Some(json!({
                "field": "walletAddresses",
                "max": MAX_EVM_WALLETS,
                "got": req.wallet_addresses.len()
            })),
        ));
    }

    if req.solana_wallet_addresses.len() > MAX_SOL_WALLETS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!("Too many Solana wallets (max {})", MAX_SOL_WALLETS),
            Some(json!({
                "field": "solanaWalletAddresses",
                "max": MAX_SOL_WALLETS,
                "got": req.solana_wallet_addresses.len()
            })),
        ));
    }

    if req.tron_wallet_addresses.len() > MAX_TRON_WALLETS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!("Too many TRON wallets (max {})", MAX_TRON_WALLETS),
            Some(json!({
                "field": "tronWalletAddresses",
                "max": MAX_TRON_WALLETS,
                "got": req.tron_wallet_addresses.len()
            })),
        ));
    }

    if req.contracts.len() > MAX_CONTRACT_GROUPS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!("Too many contract groups (max {})", MAX_CONTRACT_GROUPS),
            Some(json!({
                "field": "contracts",
                "max": MAX_CONTRACT_GROUPS,
                "got": req.contracts.len()
            })),
        ));
    }

    // Per-group contract address cap
    for (i, cg) in req.contracts.iter().enumerate() {
        if cg.contract_addresses.len() > MAX_CONTRACTS_PER_GROUP {
            return Err(ApiError::bad_request(
                "INVALID_REQUEST_LIMITS",
                format!(
                    "Too many contract addresses in contracts[{}] (max {})",
                    i, MAX_CONTRACTS_PER_GROUP
                ),
                Some(json!({
                    "field": format!("contracts[{}].contractAddresses", i),
                    "max": MAX_CONTRACTS_PER_GROUP,
                    "got": cg.contract_addresses.len(),
                    "network": cg.network_name
                })),
            ));
        }
    }

    // raw-request tron contract validation (nice early rejection)
    let mut invalid_tron_contracts: Vec<String> = vec![];
    for cg in &req.contracts {
        if cg.network_name.trim().eq_ignore_ascii_case("trx") {
            for c in &cg.contract_addresses {
                if !is_valid_tron_base58_addr(c) {
                    invalid_tron_contracts.push(c.clone());
                }
            }
        }
    }

    if !invalid_tron_contracts.is_empty() {
        invalid_tron_contracts.sort();
        invalid_tron_contracts.dedup();
        return Err(
            ApiError::bad_request(
                "INVALID_TRON_CONTRACT",
                "One or more TRON contract addresses are invalid (expected base58check TRON address starting with T)",
                Some(
                    json!({
                "field": "contracts[trx].contractAddresses",
                "invalid": invalid_tron_contracts
            })
                )
            )
        );
    }

    // raw-request tron wallet validation (early rejection)
    let mut invalid_tron_wallets: Vec<String> = vec![];
    for w in &req.tron_wallet_addresses {
        if !is_valid_tron_base58_addr(w) {
            invalid_tron_wallets.push(w.clone());
        }
    }
    if !invalid_tron_wallets.is_empty() {
        invalid_tron_wallets.sort();
        invalid_tron_wallets.dedup();
        return Err(
            ApiError::bad_request(
                "INVALID_TRON_WALLET",
                "One or more TRON wallet addresses are invalid (expected base58check TRON address starting with T)",
                Some(
                    json!({
                "field": "tronWalletAddresses",
                "invalid": invalid_tron_wallets
            })
                )
            )
        );
    }

    Ok(())
}

/// Validate normalized request (addresses + networks + work units)
pub fn validate_normalized_request(req: &BalanceRequest) -> Result<(), ApiError> {
    validate_networks(req)?;
    validate_evm_wallets(req)?;
    validate_solana_wallets(req)?;
    validate_tron_wallets(req)?;
    validate_contract_groups(req)?;
    validate_work_units(req)?;
    Ok(())
}

fn validate_networks(req: &BalanceRequest) -> Result<(), ApiError> {
    let evm = supported_evm_networks();
    let mut unknown: Vec<String> = vec![];

    for cg in &req.contracts {
        let n = cg.network_name.as_str();
        if n == "sol" || n == "trx" {
            continue;
        }
        if !evm.contains_key(n) {
            unknown.push(cg.network_name.clone());
        }
    }

    if !unknown.is_empty() {
        unknown.sort();
        unknown.dedup();
        return Err(ApiError::bad_request(
            "UNSUPPORTED_NETWORK",
            "One or more networks in contracts are not supported",
            Some(json!({
                "field": "contracts[].networkName",
                "unsupported": unknown
            })),
        ));
    }

    Ok(())
}

fn validate_evm_wallets(req: &BalanceRequest) -> Result<(), ApiError> {
    let mut invalid: Vec<String> = vec![];

    for w in &req.wallet_addresses {
        if !is_valid_evm_h160(w) {
            invalid.push(w.clone());
        }
    }

    if !invalid.is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_EVM_WALLET",
            "One or more EVM wallet addresses are invalid (expected 20-byte hex address)",
            Some(json!({
                "field": "walletAddresses",
                "invalid": invalid
            })),
        ));
    }

    Ok(())
}

fn validate_solana_wallets(req: &BalanceRequest) -> Result<(), ApiError> {
    let mut invalid: Vec<String> = vec![];

    for w in &req.solana_wallet_addresses {
        if !is_valid_solana_pubkey_32(w) {
            invalid.push(w.clone());
        }
    }

    if !invalid.is_empty() {
        return Err(
            ApiError::bad_request(
                "INVALID_SOL_WALLET",
                "One or more Solana wallet addresses are invalid base58 pubkeys (must decode to 32 bytes)",
                Some(
                    json!({
                "field": "solanaWalletAddresses",
                "invalid": invalid
            })
                )
            )
        );
    }

    Ok(())
}

fn validate_tron_wallets(req: &BalanceRequest) -> Result<(), ApiError> {
    let mut invalid: Vec<String> = vec![];

    for w in &req.tron_wallet_addresses {
        if !is_valid_tron_base58_addr(w) {
            invalid.push(w.clone());
        }
    }

    if !invalid.is_empty() {
        return Err(
            ApiError::bad_request(
                "INVALID_TRON_WALLET",
                "One or more TRON wallet addresses are invalid (expected base58check TRON address starting with T)",
                Some(
                    json!({
                "field": "tronWalletAddresses",
                "invalid": invalid
            })
                )
            )
        );
    }

    Ok(())
}

fn validate_contract_groups(req: &BalanceRequest) -> Result<(), ApiError> {
    let mut invalid_evm_contracts: Vec<String> = vec![];
    let mut invalid_sol_mints: Vec<String> = vec![];
    let mut invalid_tron_contracts: Vec<String> = vec![];

    for cg in &req.contracts {
        let net = cg.network_name.as_str();

        if net == "sol" {
            for mint in &cg.contract_addresses {
                if !is_valid_solana_pubkey_32(mint) {
                    invalid_sol_mints.push(mint.clone());
                }
            }
        } else if net == "trx" {
            for c in &cg.contract_addresses {
                if !is_valid_tron_base58_addr(c) {
                    invalid_tron_contracts.push(c.clone());
                }
            }
        } else {
            for c in &cg.contract_addresses {
                if !is_valid_evm_h160(c) {
                    invalid_evm_contracts.push(c.clone());
                }
            }
        }
    }

    if !invalid_evm_contracts.is_empty() {
        invalid_evm_contracts.sort();
        invalid_evm_contracts.dedup();
        return Err(ApiError::bad_request(
            "INVALID_EVM_CONTRACT",
            "One or more EVM token contract addresses are invalid (expected 20-byte hex address)",
            Some(json!({
                "field": "contracts[].contractAddresses",
                "invalid": invalid_evm_contracts
            })),
        ));
    }

    if !invalid_sol_mints.is_empty() {
        invalid_sol_mints.sort();
        invalid_sol_mints.dedup();
        return Err(
            ApiError::bad_request(
                "INVALID_SOL_MINT",
                "One or more Solana mint addresses are invalid base58 pubkeys (must decode to 32 bytes)",
                Some(
                    json!({
                "field": "contracts[sol].contractAddresses",
                "invalid": invalid_sol_mints
            })
                )
            )
        );
    }

    if !invalid_tron_contracts.is_empty() {
        invalid_tron_contracts.sort();
        invalid_tron_contracts.dedup();
        return Err(
            ApiError::bad_request(
                "INVALID_TRON_CONTRACT",
                "One or more TRON contract addresses are invalid (expected base58check TRON address starting with T)",
                Some(
                    json!({
                "field": "contracts[trx].contractAddresses",
                "invalid": invalid_tron_contracts
            })
                )
            )
        );
    }

    Ok(())
}

fn validate_work_units(req: &BalanceRequest) -> Result<(), ApiError> {
    // EVM token count excludes both sol + trx
    let evm_tokens_total: usize = req
        .contracts
        .iter()
        .filter(|c| {
            let n = c.network_name.as_str();
            n != "sol" && n != "trx"
        })
        .map(|c| c.contract_addresses.len())
        .sum();

    let evm_work = req.wallet_addresses.len().saturating_mul(evm_tokens_total);
    if evm_work > MAX_EVM_WORK_UNITS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!(
                "EVM request too large (wallets * tokens > {})",
                MAX_EVM_WORK_UNITS
            ),
            Some(json!({
                "field": "walletAddresses/contracts",
                "wallets": req.wallet_addresses.len(),
                "tokens_total": evm_tokens_total,
                "work_units": evm_work,
                "max_work_units": MAX_EVM_WORK_UNITS
            })),
        ));
    }

    // SOL work = sol_wallets * sol_mints
    let sol_mints_total: usize = req
        .contracts
        .iter()
        .find(|c| c.network_name.as_str() == "sol")
        .map(|c| c.contract_addresses.len())
        .unwrap_or(0);

    let sol_work = req
        .solana_wallet_addresses
        .len()
        .saturating_mul(sol_mints_total);
    if sol_work > MAX_SOL_WORK_UNITS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!(
                "Solana request too large (wallets * mints > {})",
                MAX_SOL_WORK_UNITS
            ),
            Some(json!({
                "field": "solanaWalletAddresses/contracts[sol]",
                "wallets": req.solana_wallet_addresses.len(),
                "mints_total": sol_mints_total,
                "work_units": sol_work,
                "max_work_units": MAX_SOL_WORK_UNITS
            })),
        ));
    }

    // ✅ TRON work:
    // If tronWalletAddresses provided, use that.
    // Else if TRX contracts requested, assume TRON wallets are derived from EVM walletAddresses.
    let tron_contracts_total: usize = req
        .contracts
        .iter()
        .find(|c| c.network_name.as_str() == "trx")
        .map(|c| c.contract_addresses.len())
        .unwrap_or(0);

    let tron_wallets_effective = if !req.tron_wallet_addresses.is_empty() {
        req.tron_wallet_addresses.len()
    } else if tron_contracts_total > 0 {
        req.wallet_addresses.len()
    } else {
        0
    };

    let tron_work = tron_wallets_effective.saturating_mul(tron_contracts_total);

    if tron_work > MAX_TRON_WORK_UNITS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!(
                "TRON request too large (wallets * contracts > {})",
                MAX_TRON_WORK_UNITS
            ),
            Some(json!({
                "field": "tronWalletAddresses/contracts[trx]",
                "wallets_effective": tron_wallets_effective,
                "wallets_tron": req.tron_wallet_addresses.len(),
                "wallets_evm": req.wallet_addresses.len(),
                "contracts_total": tron_contracts_total,
                "work_units": tron_work,
                "max_work_units": MAX_TRON_WORK_UNITS
            })),
        ));
    }

    Ok(())
}

fn is_valid_evm_h160(s: &str) -> bool {
    let t = s.trim();
    let hex = t.strip_prefix("0x").unwrap_or(t);

    if hex.len() != 40 {
        return false;
    }

    hex.chars().all(|c| c.is_ascii_hexdigit())
}

fn is_valid_solana_pubkey_32(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }

    let decoded = match bs58::decode(t).into_vec() {
        Ok(v) => v,
        Err(_) => {
            return false;
        }
    };

    decoded.len() == 32
}

/// Small helper: find a contract group by network
#[allow(dead_code)]
fn _find_group<'a>(contracts: &'a [ContractGroup], network: &str) -> Option<&'a ContractGroup> {
    contracts
        .iter()
        .find(|c| c.network_name.as_str() == network)
}
