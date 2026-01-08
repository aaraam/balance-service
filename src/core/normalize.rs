// ==================================================
// balance-service\src\core\normalize.rs
// ==================================================

use crate::http::dto::{BalanceRequest, ContractGroup};

fn lc(s: &str) -> String {
    s.trim().to_lowercase()
}

// Networks we explicitly IGNORE even if client sends them.
fn is_ignored_network(net: &str) -> bool {
    matches!(net, "trx" | "sol" | "btc" | "doge")
}

pub fn normalize_request(req: &BalanceRequest) -> BalanceRequest {
    // Normalize EVM wallets (hex addresses only; you can tighten later)
    let mut evm_wallets: Vec<String> = req.wallet_addresses.iter().map(|w| lc(w)).collect();
    evm_wallets.sort();
    evm_wallets.dedup();

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

    // Non-EVM lists: explicitly ignored -> always empty
    BalanceRequest {
        hard_refresh: req.hard_refresh,
        contracts,
        wallet_addresses: evm_wallets,
        solana_wallet_addresses: vec![],
        doge_wallet_addresses: vec![],
        btc_wallet_addresses: vec![],
    }
}
