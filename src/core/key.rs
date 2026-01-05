use sha2::{Digest, Sha256};

pub fn request_key_from_canonical_json(canonical_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_json.as_bytes());
    hex::encode(hasher.finalize())
}
