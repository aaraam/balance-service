// ==================================================
// balance-service\src\core\normalize.rs
// ==================================================

use crate::chains::is_ignored_network;
use crate::http::dto::{BalanceRequest, ContractGroup};

fn lc(s: &str) -> String {
    s.trim().to_lowercase()
}

pub fn normalize_request(req: &BalanceRequest) -> BalanceRequest {
    // Normalize EVM wallets
    let mut evm_wallets: Vec<String> = req.wallet_addresses.iter().map(|w| lc(w)).collect();
    evm_wallets.sort();
    evm_wallets.dedup();

    // Normalize Solana wallets (base58 -> we just trim/lowercase? actually base58 is case-sensitive)
    // IMPORTANT: DO NOT lowercase Solana addresses. Only trim.
    let mut sol_wallets: Vec<String> = req
        .solana_wallet_addresses
        .iter()
        .map(|w| w.trim().to_string())
        .collect();
    sol_wallets.sort();
    sol_wallets.dedup();

    // Normalize contracts per network, and DROP ignored networks entirely
    let mut contracts: Vec<ContractGroup> = req
        .contracts
        .iter()
        .map(|c| {
            let mut addrs: Vec<String> = c.contract_addresses.iter().map(|a| lc(a)).collect();
            addrs.sort();
            addrs.dedup();

            ContractGroup {
                network_name: lc(&c.network_name),
                contract_addresses: addrs,
            }
        })
        .filter(|c| !is_ignored_network(c.network_name.as_str()))
        .collect();

    contracts.sort_by(|a, b| a.network_name.cmp(&b.network_name));

    BalanceRequest {
        hard_refresh: req.hard_refresh,
        contracts,
        wallet_addresses: evm_wallets,
        solana_wallet_addresses: sol_wallets,
        doge_wallet_addresses: vec![],
        btc_wallet_addresses: vec![],
    }
}
