// ==================================================
// FILE: src/core/chains_meta.rs
//
// Single source of truth for chain metadata that is
// needed outside of chains.rs (e.g. native gas token
// symbols). Both http/dto.rs and worker/runner.rs
// import from here — never define this locally again.
// ==================================================

/// Returns the native gas token symbol for a given network key.
///
/// This is the canonical mapping used when building the balance
/// response object. The key written into the JSON result is what
/// the frontend reads, so changes here affect the API contract.
///
/// Rules applied:
///   - ETH-native L1 + all OP-Stack / Arbitrum / other ETH-L2s  → "eth"
///   - Mantle uses MNT (its own gas token), NOT ETH              → "mnt"
///   - Every other L1 uses its own well-known symbol
///   - Fallback: return the network key itself (safe default)
pub fn native_symbol_for(network: &str) -> &str {
    match network {
        // ── ETH-native chains ──────────────────────────────────────────
        "eth" | "op" | "base" | "arb1" | "linea" | "aurora" | "mint" => "eth",

        // ── Mantle: uses MNT as native gas token, NOT ETH ──────────────
        "mantle" => "mnt",

        // ── Independent L1s with their own native token ────────────────
        "bnb"      => "bnb",
        "matic"    => "matic",
        "avax"     => "avax",
        "ftm"      => "ftm",
        "cro"      => "cro",
        "gnosis"   => "xdai",
        "rstk"     => "rbtc",
        "ethc"     => "etc",
        "heco"     => "ht",
        "cypress"  => "klay",
        "iotex"    => "iotx",
        "okxchain" => "okt",
        "callisto" => "clo",
        "palm"     => "palm",
        "mcardano" => "milkada",

        // ── Non-EVM (safe fallback)
        "sol" => "sol",
        "trx" => "trx",

        // ── Fallback
        _ => network,
    }
}