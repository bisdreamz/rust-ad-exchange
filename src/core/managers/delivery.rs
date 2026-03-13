use crate::core::models::common::DeliveryState;
use firestore::FirestoreDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

#[derive(Serialize, Deserialize)]
struct DeliveryStateUpdate {
    delivery_state: DeliveryState,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<crate::core::models::common::Status>,
}

/// Write delivery state changes to Firestore for a given collection.
/// Shared by both campaign and deal managers.
pub(crate) async fn write_delivery_states(
    db: Arc<FirestoreDb>,
    collection: &str,
    changes: Vec<(String, DeliveryState)>,
    pause_on_total_exhausted: bool,
) {
    for (id, state) in changes {
        let status = if pause_on_total_exhausted && state == DeliveryState::TotalBudgetExhausted {
            Some(crate::core::models::common::Status::Paused)
        } else {
            None
        };

        let update = DeliveryStateUpdate {
            delivery_state: state,
            status,
        };

        let fields: Vec<String> = if update.status.is_some() {
            vec!["delivery_state".into(), "status".into()]
        } else {
            vec!["delivery_state".into()]
        };

        let result = db
            .fluent()
            .update()
            .fields(fields)
            .in_col(collection)
            .document_id(&id)
            .object(&update)
            .execute::<()>()
            .await;

        if let Err(e) = result {
            warn!(%id, collection, error = %e, "Failed to write delivery state to Firestore");
        }
    }
}
