use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChainKey {
    Eth,
    Bnb,
    Matic,
    Op,

    // thirdweb service (inactive now but supported)
    Gnosis,
    Rstk,
    Ethc,
    Linea,
    Base,
    Mantle,
    Mint,
    Heco,
    Avax,
    Ftm,
    Cro,
    Arb1,
    Palm,
    Cypress,
    Iotex,
    Aurora,
    Mcardano,
    Okxchain,
    Callisto,
}

impl ChainKey {
    pub fn from_network_name(s: &str) -> Option<Self> {
        match s {
            "eth" => Some(Self::Eth),
            "bnb" => Some(Self::Bnb),
            "matic" => Some(Self::Matic),
            "op" => Some(Self::Op),

            "gnosis" => Some(Self::Gnosis),
            "rstk" => Some(Self::Rstk),
            "ethc" => Some(Self::Ethc),
            "linea" => Some(Self::Linea),
            "base" => Some(Self::Base),
            "mantle" => Some(Self::Mantle),
            "mint" => Some(Self::Mint),
            "heco" => Some(Self::Heco),
            "avax" => Some(Self::Avax),
            "ftm" => Some(Self::Ftm),
            "cro" => Some(Self::Cro),
            "arb1" => Some(Self::Arb1),
            "palm" => Some(Self::Palm),
            "cypress" => Some(Self::Cypress),
            "iotex" => Some(Self::Iotex),
            "aurora" => Some(Self::Aurora),
            "mcardano" => Some(Self::Mcardano),
            "okxchain" => Some(Self::Okxchain),
            "callisto" => Some(Self::Callisto),

            _ => None,
        }
    }

    pub fn chain_id(&self) -> u64 {
        match self {
            Self::Eth => 1,
            Self::Bnb => 56,
            Self::Matic => 137,
            Self::Op => 10,

            Self::Gnosis => 100,
            Self::Rstk => 30,
            Self::Ethc => 61,
            Self::Linea => 59144,
            Self::Base => 8453,
            Self::Mantle => 5000,
            Self::Mint => 185,
            Self::Heco => 128,
            Self::Avax => 43114,
            Self::Ftm => 250,
            Self::Cro => 25,
            Self::Arb1 => 42161,
            Self::Palm => 11297108109,
            Self::Cypress => 8217,
            Self::Iotex => 4689,
            Self::Aurora => 18869,
            Self::Mcardano => 2001,
            Self::Okxchain => 66,
            Self::Callisto => 820,
        }
    }

    pub fn thirdweb_rpc_url(&self, client_id: &str) -> String {
        format!("https://{}.rpc.thirdweb.com/{}", self.chain_id(), client_id)
    }
}

pub fn supported_evm_networks() -> HashMap<&'static str, ChainKey> {
    HashMap::from([
        ("eth", ChainKey::Eth),
        ("bnb", ChainKey::Bnb),
        ("matic", ChainKey::Matic),
        ("op", ChainKey::Op),

        ("gnosis", ChainKey::Gnosis),
        ("rstk", ChainKey::Rstk),
        ("ethc", ChainKey::Ethc),
        ("linea", ChainKey::Linea),
        ("base", ChainKey::Base),
        ("mantle", ChainKey::Mantle),
        ("mint", ChainKey::Mint),
        ("heco", ChainKey::Heco),
        ("avax", ChainKey::Avax),
        ("ftm", ChainKey::Ftm),
        ("cro", ChainKey::Cro),
        ("arb1", ChainKey::Arb1),
        ("palm", ChainKey::Palm),
        ("cypress", ChainKey::Cypress),
        ("iotex", ChainKey::Iotex),
        ("aurora", ChainKey::Aurora),
        ("mcardano", ChainKey::Mcardano),
        ("okxchain", ChainKey::Okxchain),
        ("callisto", ChainKey::Callisto),
    ])
}

/// Non-EVM chains we currently "support" only as zero-balance stubs.
pub fn is_non_evm_stub(network_name: &str) -> bool {
    matches!(network_name, "trx" | "sol" | "btc" | "doge")
}
