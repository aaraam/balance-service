use crate::db::models::CryptoMarketTrackedTokenDoc;
use bson::{doc, to_bson, DateTime};
use futures::TryStreamExt;
use mongodb::Collection;

pub fn crypto_market_tracked_tokens_collection(
    db: &mongodb::Database,
) -> Collection<CryptoMarketTrackedTokenDoc> {
    db.collection::<CryptoMarketTrackedTokenDoc>("crypto_market_tracked_tokens")
}

pub async fn ensure_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    use mongodb::{options::IndexOptions, IndexModel};

    let coll = crypto_market_tracked_tokens_collection(db);
    let model = IndexModel::builder()
        .keys(doc! { "currency": 1, "trackingKey": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();

    coll.create_index(model).await?;
    Ok(())
}

pub async fn list_enabled_by_currency(
    db: &mongodb::Database,
    currency: &str,
) -> Result<Vec<CryptoMarketTrackedTokenDoc>, mongodb::error::Error> {
    let coll = crypto_market_tracked_tokens_collection(db);
    let mut cursor = coll
        .find(doc! {
            "currency": currency,
            "enabled": true
        })
        .await?;

    let mut out = Vec::new();
    while let Some(doc) = cursor.try_next().await? {
        out.push(doc);
    }

    Ok(out)
}

pub async fn upsert_token(
    db: &mongodb::Database,
    token: &CryptoMarketTrackedTokenDoc,
) -> Result<CryptoMarketTrackedTokenDoc, mongodb::error::Error> {
    let coll = crypto_market_tracked_tokens_collection(db);
    let now = DateTime::now();
    let asset_platform_id = to_bson(&token.asset_platform_id).unwrap_or(bson::Bson::Null);
    let contract_address = to_bson(&token.contract_address).unwrap_or(bson::Bson::Null);
    let token_addresses = to_bson(&token.token_addresses).unwrap_or(bson::Bson::Array(vec![]));

    coll.update_one(
        doc! {
            "currency": &token.currency,
            "trackingKey": &token.tracking_key
        },
        doc! {
            "$set": {
                "coingeckoId": &token.coingecko_id,
                "symbol": &token.symbol,
                "assetPlatformId": asset_platform_id,
                "contractAddress": contract_address,
                "tokenAddresses": token_addresses,
                "enabled": token.enabled,
                "updatedAt": now,
            },
            "$setOnInsert": {
                "currency": &token.currency,
                "trackingKey": &token.tracking_key,
                "createdAt": token.created_at,
            }
        },
    )
    .upsert(true)
    .await?;

    coll.find_one(doc! {
        "currency": &token.currency,
        "trackingKey": &token.tracking_key
    })
    .await
    .map(|doc| doc.unwrap_or_else(|| token.clone()))
}
