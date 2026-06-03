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

pub fn is_valid_tron_address(s: &str) -> bool {
    use sha2::{Digest, Sha256};

    let t = s.trim();
    if t.is_empty() || !t.starts_with('T') {
        return false;
    }

    let decoded = match bs58::decode(t).into_vec() {
        Ok(v) => v,
        Err(_) => return false,
    };
    if decoded.len() != 25 {
        return false;
    }

    let (payload, checksum) = decoded.split_at(21);
    if payload[0] != 0x41 {
        return false;
    }

    let h1 = Sha256::digest(payload);
    let h2 = Sha256::digest(&h1);
    checksum == &h2[..4]
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

    // 1. walletAddresses is STRICTLY EVM-only now
    let mut wallet_addresses: Vec<String> = req
        .wallet_addresses
        .iter()
        .map(|w| w.trim().to_lowercase())
        .filter(|w| is_valid_evm_h160(w))
        .collect();

    wallet_addresses.sort();
    wallet_addresses.dedup();

    // 2. Solana stays separate
    let mut sol_wallets: Vec<String> = req
        .solana_wallet_addresses
        .iter()
        .map(|w| w.trim().to_string())
        .filter(|w| is_valid_solana_pubkey_32(w))
        .collect();

    sol_wallets.sort();
    sol_wallets.dedup();

    // 3. We no longer care about explicitly passed TRON wallets for this flow
    // But we keep the field empty to satisfy the struct
    let tron_wallets: Vec<String> = vec![];

    // 4. Contracts (Your existing logic here is already correct and preserves "trx")
    let mut contracts: Vec<ContractGroup> = req
        .contracts
        .iter()
        .filter_map(|c| {
            let net = c.network_name.to_lowercase();

            if is_ignored_network(net.as_str()) {
                return None;
            }

            if net == "sol" {
                let mut addrs: Vec<String> = c
                    .contract_addresses
                    .iter()
                    .map(|a| a.trim().to_string())
                    .filter(|a| is_valid_solana_pubkey_32(a))
                    .collect();
                addrs.sort();
                addrs.dedup();
                return if addrs.is_empty() {
                    None
                } else {
                    Some(ContractGroup {
                        network_name: net,
                        contract_addresses: addrs,
                    })
                };
            }

            if net == "trx" {
                let mut addrs: Vec<String> = c
                    .contract_addresses
                    .iter()
                    .map(|a| a.trim().to_string())
                    .filter(|a| is_valid_tron_address(a))
                    .collect();
                addrs.sort();
                addrs.dedup();
                return if addrs.is_empty() {
                    None
                } else {
                    Some(ContractGroup {
                        network_name: net,
                        contract_addresses: addrs,
                    })
                };
            }

            if !evm_supported.contains_key(net.as_str()) {
                return None;
            }

            let mut addrs: Vec<String> = c
                .contract_addresses
                .iter()
                .map(|a| a.trim().to_lowercase())
                .filter(|a| is_valid_evm_h160(a))
                .collect();
            addrs.sort();
            addrs.dedup();

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
        wallet_addresses,
        solana_wallet_addresses: sol_wallets,
        tron_wallet_addresses: tron_wallets,
        doge_wallet_addresses: vec![],
        btc_wallet_addresses: vec![],
    }
}
