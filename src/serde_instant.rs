use serde::{Serializer, Deserializer, Deserialize};
use std::time::{Duration, Instant};

pub mod instant_serde {
    use super::*;

    pub fn serialize<S>(_instant: &Instant, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Store as microseconds since Unix epoch
        let now = std::time::SystemTime::now();
        let duration_since_epoch = now.duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0));
        let micros = duration_since_epoch.as_micros() as u64;
        serializer.serialize_u64(micros)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Instant, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = u64::deserialize(deserializer)?;
        let duration = Duration::from_micros(micros);
        let system_time = std::time::UNIX_EPOCH + duration;

        // Fixed: Added Ok() wrapper
        Ok(system_time.elapsed()
            .map(|elapsed| Instant::now() - elapsed)
            .unwrap_or_else(|_| Instant::now()))
    }
}