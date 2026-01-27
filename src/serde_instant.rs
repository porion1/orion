use serde::{Serializer, Deserializer, Deserialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod instant_serde {
    use super::*;

    /// Serialize `SystemTime` as microseconds since UNIX_EPOCH
    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration_since_epoch = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0));
        let micros = duration_since_epoch.as_micros() as u64;
        serializer.serialize_u64(micros)
    }

    /// Deserialize `SystemTime` from microseconds since UNIX_EPOCH
    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_micros(micros))
    }
}
