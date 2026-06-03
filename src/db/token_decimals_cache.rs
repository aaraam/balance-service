use crate::db::models::TokenDecimalsCacheDoc;
use bson::{doc, DateTime};
use futures::TryStreamExt;
use mongodb::Collection;
use std::collections::HashMap;

pub fn token_decimals_cache_collection(
    db: &mongodb::Database,
) -> Collection<TokenDecimalsCacheDoc> {
    db.collection::<TokenDecimalsCacheDoc>("token_decimals_cache")
}

pub async fn ensure_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    use mongodb::{options::IndexOptions, IndexModel};

    let coll = token_decimals_cache_collection(db);
    let model = IndexModel::builder()
        .keys(doc! { "blockchain": 1, "contractAddress": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();

    coll.create_index(model).await?;
    Ok(())
}

pub async fn get_cached(
    db: &mongodb::Database,
    blockchain: &str,
    contract_address: &str,
) -> Result<Option<TokenDecimalsCacheDoc>, mongodb::error::Error> {
    let coll = token_decimals_cache_collection(db);
    coll.find_one(doc! {
        "blockchain": blockchain,
        "contractAddress": contract_address
    })
    .await
}

pub async fn get_many(
    db: &mongodb::Database,
    blockchain: &str,
    contract_addresses: &[String],
) -> Result<HashMap<String, u32>, mongodb::error::Error> {
    if contract_addresses.is_empty() {
        return Ok(HashMap::new());
    }

    let coll = token_decimals_cache_collection(db);
    let mut cursor = coll
        .find(doc! {
            "blockchain": blockchain,
            "contractAddress": { "$in": contract_addresses },
            "exists": true
        })
        .await?;

    let mut out = HashMap::new();
    while let Some(doc) = cursor.try_next().await? {
        if let Some(decimals) = doc.decimals {
            out.insert(doc.contract_address, decimals);
        }
    }

    Ok(out)
}

pub async fn upsert(
    db: &mongodb::Database,
    blockchain: &str,
    contract_address: &str,
    decimals: Option<u32>,
) -> Result<(), mongodb::error::Error> {
    let coll = token_decimals_cache_collection(db);
    let doc = TokenDecimalsCacheDoc {
        blockchain: blockchain.to_string(),
        contract_address: contract_address.to_string(),
        exists: decimals.is_some(),
        decimals,
        updated_at: DateTime::now(),
    };

    coll.replace_one(
        doc! {
            "blockchain": blockchain,
            "contractAddress": contract_address
        },
        doc,
    )
    .upsert(true)
    .await?;

    Ok(())
}

pub async fn upsert_many_existing(
    db: &mongodb::Database,
    blockchain: &str,
    decimals_by_contract: &HashMap<String, u32>,
) {
    for (contract, decimals) in decimals_by_contract {
        if let Err(e) = upsert(db, blockchain, contract, Some(*decimals)).await {
            tracing::warn!(
                blockchain = %blockchain,
                contract = %contract,
                error = %e,
                "failed to cache token decimals"
            );
        }
    }
}
