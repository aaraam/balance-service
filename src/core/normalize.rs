use crate::http::dto::{BalanceRequest, ContractGroup};

fn lc(s: &str) -> String {
    s.trim().to_lowercase()
}

pub fn normalize_request(req: &BalanceRequest) -> BalanceRequest {
    // Normalize wallets
    let mut wallets: Vec<String> = req.walletAddresses.iter().map(|w| lc(w)).collect();
    wallets.sort();
    wallets.dedup();

    // Normalize contracts per network
    let mut contracts: Vec<ContractGroup> = req
        .contracts
        .iter()
        .map(|c| {
            let mut addrs: Vec<String> = c.contractAddresses.iter().map(|a| lc(a)).collect();
            addrs.sort();
            addrs.dedup();

            ContractGroup {
                networkName: lc(&c.networkName),
                contractAddresses: addrs,
            }
        })
        .collect();

    // sort by networkName
    contracts.sort_by(|a, b| a.networkName.cmp(&b.networkName));

    BalanceRequest {
        contracts,
        walletAddresses: wallets,
    }
}
