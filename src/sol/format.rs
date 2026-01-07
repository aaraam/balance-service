/// Convert u128 base-units into decimal string.
/// - decimals: token decimals (SOL = 9; SPL varies)
/// - trim: if true, trim trailing zeros from fractional part (nice for native SOL)
pub fn u128_to_decimal_string(value: u128, decimals: u32, trim: bool) -> String {
    if decimals == 0 {
        return value.to_string();
    }

    let base = pow10_u128(decimals);
    if base == 0 {
        // overflow safety fallback; should never happen for sane decimals (<= 38 for u128)
        return value.to_string();
    }

    let whole = value / base;
    let frac = value % base;

    let mut frac_str = frac.to_string();
    let width = decimals as usize;

    if frac_str.len() < width {
        let pad = "0".repeat(width - frac_str.len());
        frac_str = format!("{}{}", pad, frac_str);
    }

    if trim {
        while frac_str.ends_with('0') {
            frac_str.pop();
        }
        if frac_str.is_empty() {
            return whole.to_string();
        }
    }

    format!("{}.{}", whole, frac_str)
}

fn pow10_u128(decimals: u32) -> u128 {
    let mut x: u128 = 1;
    for _ in 0..decimals {
        match x.checked_mul(10) {
            Some(v) => {
                x = v;
            }
            None => {
                return 0;
            } // overflow marker
        }
    }
    x
}
