use crate::config::AppConfig;
use mongodb::{Client, Database};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MongoError {
    #[error("mongodb error: {0}")]
    Mongo(#[from] mongodb::error::Error),
}

#[derive(Clone)]
pub struct Mongo {
    pub db: Database,
}

impl Mongo {
    pub async fn connect(cfg: &AppConfig) -> Result<Self, MongoError> {
        let client = Client::with_uri_str(&cfg.mongodb_uri).await?;
        let db = client.database(&cfg.mongodb_db_main);
        Ok(Self { db })
    }
}
