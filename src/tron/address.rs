use anyhow::anyhow;
use sha2::{Digest, Sha256};

/// Converts an EVM hex address (0x...) to a TRON base58check address (T...)
pub fn evm_hex_to_tron_b58(evm_addr: &str) -> Result<String, anyhow::Error> {
    let trimmed = evm_addr.trim();
    let hex_str = trimmed.strip_prefix("0x").unwrap_or(trimmed);

    let payload = hex::decode(hex_str).map_err(|e| anyhow!("Invalid hex: {}", e))?;
    if payload.len() != 20 {
        return Err(anyhow!("EVM address must be exactly 20 bytes"));
    }

    // Prepend 0x41 (TRON Mainnet Prefix)
    let mut tron_address = Vec::with_capacity(21);
    tron_address.push(0x41);
    tron_address.extend_from_slice(&payload);

    // Compute double SHA256 for checksum
    let h1 = Sha256::digest(&tron_address);
    let h2 = Sha256::digest(&h1);

    // Append 4-byte checksum
    let mut b58_payload = Vec::with_capacity(25);
    b58_payload.extend_from_slice(&tron_address);
    b58_payload.extend_from_slice(&h2[..4]);

    // Encode to base58
    Ok(bs58::encode(b58_payload).into_string())
}