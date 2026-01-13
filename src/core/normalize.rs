// ==================================================
// balance-service\src\core\normalize.rs
// ==================================================

use crate::chains::{is_ignored_network, supported_evm_networks};
use crate::http::dto::{BalanceRequest, ContractGroup};

fn lc(s: &str) -> String {
    s.trim().to_lowercase()
}

// --- local validators (keep them here to avoid coupling normalize <-> validate) ---

fn is_valid_evm_h160(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
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
        Err(_) => return false,
    };
    decoded.len() == 32
}

pub fn normalize_request(req: &BalanceRequest) -> BalanceRequest {
    let evm_supported = supported_evm_networks();

    // -----------------------
    // Normalize + SANITIZE EVM wallets
    // -----------------------
    let mut evm_wallets: Vec<String> = req
        .wallet_addresses
        .iter()
        .map(|w| lc(w))
        .filter(|w| is_valid_evm_h160(w))
        .collect();

    evm_wallets.sort();
    evm_wallets.dedup();

    // -----------------------
    // Normalize + SANITIZE Solana wallets (case-sensitive, trim only)
    // -----------------------
    let mut sol_wallets: Vec<String> = req
        .solana_wallet_addresses
        .iter()
        .map(|w| w.trim().to_string())
        .filter(|w| is_valid_solana_pubkey_32(w))
        .collect();

    sol_wallets.sort();
    sol_wallets.dedup();

    // -----------------------
    // Normalize contracts per network, DROP ignored networks,
    // and DROP unsupported networks + invalid addresses
    // -----------------------
    let mut contracts: Vec<ContractGroup> = req
        .contracts
        .iter()
        .filter_map(|c| {
            let net = lc(&c.network_name);

            // ignore these networks entirely (compat)
            if is_ignored_network(net.as_str()) {
                return None;
            }

            // only allow: "sol" OR supported EVM networks
            if net != "sol" && !evm_supported.contains_key(net.as_str()) {
                // unsupported network -> drop silently (fail-soft)
                return None;
            }

            // normalize addresses
            let mut addrs: Vec<String> = if net == "sol" {
                // Solana mint addresses are base58 and case-sensitive => DO NOT lowercase
                c.contract_addresses
                    .iter()
                    .map(|a| a.trim().to_string())
                    .filter(|a| is_valid_solana_pubkey_32(a))
                    .collect()
            } else {
                // EVM contract addresses => lowercase ok
                c.contract_addresses
                    .iter()
                    .map(|a| lc(a))
                    .filter(|a| is_valid_evm_h160(a))
                    .collect()
            };

            addrs.sort();
            addrs.dedup();

            // If group ends up empty, drop it.
            // (If you want "native only" even when tokens are empty, remove this block.)
            if addrs.is_empty() {
                return None;
            }

            Some(ContractGroup {
                network_name: net,
                contract_addresses: addrs,
            })
        })
        .collect();

    contracts.sort_by(|a, b| a.network_name.cmp(&b.network_name));

    BalanceRequest {
        hard_refresh: req.hard_refresh,
        contracts,
        wallet_addresses: evm_wallets,
        solana_wallet_addresses: sol_wallets,
        // keep for backward compat but always empty (as you already do)
        doge_wallet_addresses: vec![],
        btc_wallet_addresses: vec![],
    }
}
