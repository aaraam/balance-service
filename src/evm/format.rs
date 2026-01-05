use ethereum_types::U256;

/// 10^decimals as U256 (decimals is usually <= 18)
fn pow10(decimals: u32) -> U256 {
    let mut x = U256::from(1u64);
    for _ in 0..decimals {
        x = x * U256::from(10u64);
    }
    x
}

/// Convert U256 base-units into decimal string.
/// - decimals: token decimals (e.g., 18, 6)
/// - trim: if true, trim trailing zeros from fractional part (nice for native tokens)
pub fn u256_to_decimal_string(value: U256, decimals: u32, trim: bool) -> String {
    if decimals == 0 {
        return value.to_string();
    }

    let base = pow10(decimals);
    let whole = value / base;
    let frac = value % base;

    // pad fractional to fixed width
    let mut frac_str = frac.to_string();
    if frac_str.len() < decimals as usize {
        let pad = "0".repeat(decimals as usize - frac_str.len());
        frac_str = format!("{}{}", pad, frac_str);
    }

    if trim {
        // remove trailing zeros
        while frac_str.ends_with('0') {
            frac_str.pop();
        }
        if frac_str.is_empty() {
            return whole.to_string();
        }
    }

    format!("{}.{}", whole, frac_str)
}
