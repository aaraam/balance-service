use crate::chains::is_ignored_network;
use crate::http::dto::{BalanceRequest, ContractGroup};

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

pub fn supported_evm_networks() -> std::collections::HashMap<&'static str, &'static str> {
    let mut m = std::collections::HashMap::new();
    m.insert("eth", "Ethereum");
    m.insert("bnb", "BNB Chain");
    m.insert("matic", "Polygon");
    m.insert("op", "Optimism");
    m.insert("gnosis", "Gnosis");
    m.insert("rstk", "Rootstock");
    m.insert("ethc", "Ethereum Classic");
    m.insert("linea", "Linea");
    m.insert("base", "Base");
    m.insert("mantle", "Mantle");
    m.insert("mint", "Mint");
    m.insert("heco", "HECO");
    m.insert("avax", "Avalanche");
    m.insert("ftm", "Fantom");
    m.insert("cro", "Cronos");
    m.insert("arb1", "Arbitrum One");
    m.insert("palm", "Palm");
    m.insert("cypress", "Klaytn Cypress");
    m.insert("iotex", "IoTeX");
    m.insert("aurora", "Aurora");
    m.insert("mcardano", "Milkomeda Cardano");
    m.insert("okxchain", "OKX Chain");
    m.insert("callisto", "Callisto");
    m
}

pub fn normalize_request(req: &BalanceRequest) -> BalanceRequest {
    let evm_supported = supported_evm_networks();

    let mut wallet_addresses: Vec<String> = req
        .wallet_addresses
        .iter()
        .map(|w| w.to_lowercase())
        .filter(|w| is_valid_evm_h160(w))
        .collect();
    wallet_addresses.sort();
    wallet_addresses.dedup();

    let mut sol_wallets: Vec<String> = req
        .solana_wallet_addresses
        .iter()
        .map(|w| w.trim().to_string())
        .filter(|w| is_valid_solana_pubkey_32(w))
        .collect();
    sol_wallets.sort();
    sol_wallets.dedup();

    // TRON IS DISABLED. We clear any TRON input immediately.
    let tron_wallets: Vec<String> = Vec::new();

    let mut contracts: Vec<ContractGroup> = req
        .contracts
        .iter()
        .filter_map(|c| {
            let net = c.network_name.to_lowercase();

            if is_ignored_network(net.as_str()) || net == "trx" {
                return None;
            }

            if net != "sol" && !evm_supported.contains_key(net.as_str()) {
                return None;
            }

            let mut addrs: Vec<String> = if net == "sol" {
                c.contract_addresses
                    .iter()
                    .map(|a| a.trim().to_string())
                    .filter(|a| is_valid_solana_pubkey_32(a))
                    .collect()
            } else {
                c.contract_addresses
                    .iter()
                    .map(|a| a.to_lowercase())
                    .filter(|a| is_valid_evm_h160(a))
                    .collect()
            };
            addrs.sort();
            addrs.dedup();

            Some(ContractGroup {
                network_name: net,
                contract_addresses: addrs,
            })
        })
        .filter(|cg| !cg.contract_addresses.is_empty())
        .collect();

    contracts.sort_by(|a, b| a.network_name.cmp(&b.network_name));

    BalanceRequest {
        hard_refresh: req.hard_refresh,
        contracts,
        wallet_addresses,
        solana_wallet_addresses: sol_wallets,
        tron_wallet_addresses: tron_wallets,
        doge_wallet_addresses: vec![],
        btc_wallet_addresses: vec![],
    }
}