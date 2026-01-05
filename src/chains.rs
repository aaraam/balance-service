use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChainKey {
    Eth,
    Bnb,
    Matic,
    Op,
    // keep adding later if you enable more thirdweb chains
}

impl ChainKey {
    pub fn from_network_name(s: &str) -> Option<Self> {
        match s {
            "eth" => Some(Self::Eth),
            "bnb" => Some(Self::Bnb),
            "matic" => Some(Self::Matic),
            "op" => Some(Self::Op),
            _ => None,
        }
    }

    pub fn chain_id(&self) -> u64 {
        match self {
            Self::Eth => 1,
            Self::Bnb => 56,
            Self::Matic => 137,
            Self::Op => 10,
        }
    }

    pub fn thirdweb_rpc_url(&self, client_id: &str) -> String {
        format!("https://{}.rpc.thirdweb.com/{}", self.chain_id(), client_id)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Eth => "ethereum",
            Self::Bnb => "binance",
            Self::Matic => "polygon",
            Self::Op => "optimism",
        }
    }
}

pub fn supported_evm_networks() -> HashMap<&'static str, ChainKey> {
    HashMap::from([
        ("eth", ChainKey::Eth),
        ("bnb", ChainKey::Bnb),
        ("matic", ChainKey::Matic),
        ("op", ChainKey::Op),
    ])
}

/// Non-EVM chains we currently "support" only as zero-balance stubs.
pub fn is_non_evm_stub(network_name: &str) -> bool {
    matches!(network_name, "trx" | "sol" | "btc" | "doge")
}
