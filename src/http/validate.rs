use crate::core::normalize::supported_evm_networks;
use crate::http::dto::{BalanceRequest, ContractGroup};
use crate::http::error::ApiError;
use serde_json::json;

const MAX_EVM_WALLETS: usize = 150;
const MAX_SOL_WALLETS: usize = 150;
const MAX_CONTRACT_GROUPS: usize = 25;
const MAX_CONTRACTS_PER_GROUP: usize = 250;

const MAX_EVM_WORK_UNITS: usize = 10_000;
const MAX_SOL_WORK_UNITS: usize = 10_000;

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

    for cg in &req.contracts {
        if cg.contract_addresses.len() > MAX_CONTRACTS_PER_GROUP {
            return Err(ApiError::bad_request(
                "INVALID_REQUEST_LIMITS",
                format!(
                    "Too many contracts in group '{}' (max {})",
                    cg.network_name, MAX_CONTRACTS_PER_GROUP
                ),
                Some(json!({
                    "field": "contracts[].contractAddresses",
                    "network": cg.network_name,
                    "max": MAX_CONTRACTS_PER_GROUP,
                    "got": cg.contract_addresses.len()
                })),
            ));
        }
    }

    Ok(())
}

pub fn validate_normalized_request(req: &BalanceRequest) -> Result<(), ApiError> {
    validate_networks(req)?;
    validate_evm_wallets(req)?;
    validate_solana_wallets(req)?;
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
        return Err(ApiError::bad_request(
            "INVALID_NETWORK",
            "Unknown network(s) in request",
            Some(json!({ "unknown": unknown })),
        ));
    }

    Ok(())
}

fn validate_evm_wallets(req: &BalanceRequest) -> Result<(), ApiError> {
    for w in &req.wallet_addresses {
        if !is_valid_evm_h160(w) {
            return Err(ApiError::bad_request(
                "INVALID_ADDRESS",
                "Invalid EVM wallet address",
                Some(json!({ "wallet": w })),
            ));
        }
    }
    Ok(())
}

fn validate_solana_wallets(req: &BalanceRequest) -> Result<(), ApiError> {
    for w in &req.solana_wallet_addresses {
        if !is_valid_solana_pubkey_32(w) {
            return Err(ApiError::bad_request(
                "INVALID_ADDRESS",
                "Invalid Solana wallet address",
                Some(json!({ "wallet": w })),
            ));
        }
    }
    Ok(())
}

fn validate_contract_groups(req: &BalanceRequest) -> Result<(), ApiError> {
    for cg in &req.contracts {
        validate_contract_group(cg)?;
    }
    Ok(())
}

fn validate_contract_group(cg: &ContractGroup) -> Result<(), ApiError> {
    let net = cg.network_name.as_str();

    if net == "sol" {
        for a in &cg.contract_addresses {
            if !is_valid_solana_pubkey_32(a) {
                return Err(ApiError::bad_request(
                    "INVALID_CONTRACT",
                    "Invalid Solana mint address",
                    Some(json!({ "network": net, "contract": a })),
                ));
            }
        }
        return Ok(());
    }

    if net == "trx" {
        return Ok(());
    }

    for a in &cg.contract_addresses {
        if !is_valid_evm_h160(a) {
            return Err(ApiError::bad_request(
                "INVALID_CONTRACT",
                "Invalid EVM contract address",
                Some(json!({ "network": net, "contract": a })),
            ));
        }
    }

    Ok(())
}

fn validate_work_units(req: &BalanceRequest) -> Result<(), ApiError> {
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

    let sol_contracts_total: usize = req
        .contracts
        .iter()
        .find(|c| c.network_name.as_str() == "sol")
        .map(|c| c.contract_addresses.len())
        .unwrap_or(0);

    let sol_work = req
        .solana_wallet_addresses
        .len()
        .saturating_mul(sol_contracts_total);

    if sol_work > MAX_SOL_WORK_UNITS {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST_LIMITS",
            format!(
                "Solana request too large (wallets * tokens > {})",
                MAX_SOL_WORK_UNITS
            ),
            Some(json!({
                "field": "solanaWalletAddresses/contracts",
                "wallets": req.solana_wallet_addresses.len(),
                "tokens_total": sol_contracts_total,
                "work_units": sol_work,
                "max_work_units": MAX_SOL_WORK_UNITS
            })),
        ));
    }

    Ok(())
}

fn is_valid_evm_h160(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
    let t = t.strip_prefix("0x").unwrap_or(t);
    if t.len() != 40 {
        return false;
    }
    t.chars().all(|c| c.is_ascii_hexdigit())
}

fn is_valid_solana_pubkey_32(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
    let decoded = match bs58::decode(t).into_vec() {
        Ok(v) => v,
        Err(_) => return false,
    };
    decoded.len() == 32
}
