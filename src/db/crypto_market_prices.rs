use crate::db::models::CryptoMarketPriceDoc;
use bson::doc;
use mongodb::Collection;

pub fn crypto_market_prices_collection(db: &mongodb::Database) -> Collection<CryptoMarketPriceDoc> {
    db.collection::<CryptoMarketPriceDoc>("crypto_market_prices")
}

pub async fn ensure_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    use mongodb::{options::IndexOptions, IndexModel};

    let coll = crypto_market_prices_collection(db);

    let model = IndexModel::builder()
        .keys(doc! { "currency": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();

    coll.create_index(model).await?;
    Ok(())
}

pub async fn upsert_latest(
    db: &mongodb::Database,
    snapshot: &CryptoMarketPriceDoc,
) -> Result<(), mongodb::error::Error> {
    let coll = crypto_market_prices_collection(db);

    coll.replace_one(doc! { "currency": &snapshot.currency }, snapshot)
        .upsert(true)
        .await?;

    Ok(())
}

pub async fn get_latest(
    db: &mongodb::Database,
    currency: &str,
) -> Result<Option<CryptoMarketPriceDoc>, mongodb::error::Error> {
    let coll = crypto_market_prices_collection(db);
    coll.find_one(doc! { "currency": currency }).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::db::mongo::Mongo;
    use crate::market::techbank::{fetch_market_prices, market_price_doc_from_fetch};

    #[tokio::test]
    #[ignore = "requires MongoDB env + network; writes latest USD market prices"]
    async fn live_e2e_fetches_and_persists_usd_market_prices() {
        let _ = dotenvy::from_path("D:/Learn/rust/balance-service/.env");

        let cfg = AppConfig::from_env().expect("valid AppConfig");
        let mongo = Mongo::connect(&cfg).await.expect("MongoDB connection");

        ensure_indexes(&mongo.db)
            .await
            .expect("market price indexes");

        let fetched = fetch_market_prices(&cfg.crypto_market_price_url, "usd", 30_000)
            .await
            .expect("TechBank market price fetch");
        let snapshot = market_price_doc_from_fetch(&fetched);

        assert!(snapshot.count > 0);
        assert_eq!(snapshot.count as usize, snapshot.assets.len());

        upsert_latest(&mongo.db, &snapshot)
            .await
            .expect("market price upsert");

        let saved = get_latest(&mongo.db, "usd")
            .await
            .expect("saved market price lookup")
            .expect("saved market price document");

        assert_eq!(saved.currency, "usd");
        assert_eq!(saved.count, snapshot.count);
        assert_eq!(saved.assets.len(), snapshot.assets.len());
    }
}
