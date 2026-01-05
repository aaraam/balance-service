use crate::http::dto::{BalanceRequest, ContractGroup};

fn lc(s: &str) -> String {
    s.trim().to_lowercase()
}

pub fn normalize_request(req: &BalanceRequest) -> BalanceRequest {
    // Normalize EVM wallets
    let mut evm_wallets: Vec<String> = req.wallet_addresses.iter().map(|w| lc(w)).collect();
    evm_wallets.sort();
    evm_wallets.dedup();

    // Normalize Solana wallets
    let mut sol_wallets: Vec<String> = req
        .solana_wallet_addresses
        .iter()
        .map(|w| lc(w))
        .collect();
    sol_wallets.sort();
    sol_wallets.dedup();

    // Normalize Doge wallets
    let mut doge_wallets: Vec<String> = req.doge_wallet_addresses.iter().map(|w| lc(w)).collect();
    doge_wallets.sort();
    doge_wallets.dedup();

    // Normalize BTC wallets
    let mut btc_wallets: Vec<String> = req.btc_wallet_addresses.iter().map(|w| lc(w)).collect();
    btc_wallets.sort();
    btc_wallets.dedup();

    // Normalize contracts per network
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
        .collect();

    // sort by network_name
    contracts.sort_by(|a, b| a.network_name.cmp(&b.network_name));

    BalanceRequest {
        hard_refresh: req.hard_refresh,
        contracts,
        wallet_addresses: evm_wallets,
        solana_wallet_addresses: sol_wallets,
        doge_wallet_addresses: doge_wallets,
        btc_wallet_addresses: btc_wallets,
    }
}
