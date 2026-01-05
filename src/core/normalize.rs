use crate::http::dto::{BalanceRequest, ContractGroup};

fn lc(s: &str) -> String {
    s.trim().to_lowercase()
}

pub fn normalize_request(req: &BalanceRequest) -> BalanceRequest {
    // Normalize EVM wallets (hex addresses only; you can tighten later)
    let mut evm_wallets: Vec<String> = req.wallet_addresses.iter().map(|w| lc(w)).collect();
    evm_wallets.sort();
    evm_wallets.dedup();

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

    contracts.sort_by(|a, b| a.network_name.cmp(&b.network_name));

    // Non-EVM lists: keep as-is for now (no lowercasing base58/bech32)
    let mut sol = req.solana_wallet_addresses.clone();
    sol.sort();
    sol.dedup();

    let mut doge = req.doge_wallet_addresses.clone();
    doge.sort();
    doge.dedup();

    let mut btc = req.btc_wallet_addresses.clone();
    btc.sort();
    btc.dedup();

    BalanceRequest {
        hard_refresh: req.hard_refresh,
        contracts,
        wallet_addresses: evm_wallets,
        solana_wallet_addresses: sol,
        doge_wallet_addresses: doge,
        btc_wallet_addresses: btc,
    }
}
