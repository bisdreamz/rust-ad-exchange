#[derive(Debug, Clone, Copy)]
pub enum CounterValue {
    Float(f64),
    Int(u64),
}

impl CounterValue {
    pub fn is_zero(&self) -> bool {
        match self {
            CounterValue::Int(0) => true,
            CounterValue::Float(f) => *f == 0.0,
            _ => false,
        }
    }
}

pub trait CounterBuffer: Default + Clone + Send + Sync + 'static {
    /// Merges another buffer into this one,
    /// intended for flush failure recovery
    fn merge(&mut self, other: &Self);

    /// Takes all accumulated values, resetting self to defaults.
    fn clone_and_reset(&mut self) -> Self {
        std::mem::take(self)
    }

    /// Returns counter field name/value pairs from the buffer
    fn counter_pairs(&self) -> Vec<(&'static str, CounterValue)>;
}
