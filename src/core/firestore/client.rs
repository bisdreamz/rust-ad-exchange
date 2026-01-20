use crate::app::config::FirestoreConfig;
use anyhow::Error;
use firestore::{FirestoreDb, FirestoreDbOptions};

pub async fn create_client(config: &FirestoreConfig) -> Result<FirestoreDb, Error> {
    let mut options = FirestoreDbOptions::new(config.project_id.clone());

    if let Some(db_id) = &config.database_id {
        options = options.with_database_id(db_id.clone());
    }

    if let Some(host) = &config.emulator_host {
        options = options.with_firebase_api_url(format!("http://{}", host));
    }

    let db = if let Some(path) = &config.credentials_path {
        FirestoreDb::with_options_service_account_key_file(options, path.clone()).await?
    } else {
        FirestoreDb::with_options(options).await?
    };

    Ok(db)
}
