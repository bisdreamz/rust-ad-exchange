use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Active,
    Paused,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum DeliveryState {
    #[default]
    Pending,
    Delivering,
    DailyBudgetExhausted,
    TotalBudgetExhausted,
    FlightEnded,
}
