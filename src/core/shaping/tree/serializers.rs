pub mod atomic_u64_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::sync::atomic::{AtomicU64, Ordering};

    pub fn serialize<S>(atomic: &AtomicU64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(atomic.load(Ordering::Relaxed))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<AtomicU64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Ok(AtomicU64::new(value))
    }
}

pub mod rwlock_serde {
    use crate::core::shaping::tree::handler::HandlerState;
    use parking_lot::RwLock;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(lock: &RwLock<HandlerState>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        lock.read().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<RwLock<HandlerState>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let state = HandlerState::deserialize(deserializer)?;
        Ok(RwLock::new(state))
    }
}
