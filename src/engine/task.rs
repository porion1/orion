use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::str::FromStr;
use cron::Schedule;
use chrono::Utc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    OneShot,
    Recurring { cron_expr: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub task_type: TaskType,
    #[serde(with = "system_time_serde")]
    pub scheduled_at: SystemTime,
    pub payload: Option<serde_json::Value>,
    pub status: TaskStatus,
    #[serde(with = "system_time_serde")]
    pub created_at: SystemTime,
    #[serde(with = "system_time_serde")]
    pub updated_at: SystemTime,
    pub retry_count: u32,
    pub max_retries: u32,
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

mod system_time_serde {
    use serde::{Serializer, Deserializer, Deserialize};
    use std::time::{SystemTime, Duration, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

impl Task {
    pub fn new_one_shot(name: &str, delay: Duration, payload: Option<serde_json::Value>) -> Self {
        let now = SystemTime::now();
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::OneShot,
            scheduled_at: now.checked_add(delay).unwrap_or(now),
            payload,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: 3,
            metadata: None,
        }
    }

    pub fn new_recurring(name: &str, cron_expr: &str, payload: Option<serde_json::Value>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let schedule = Schedule::from_str(cron_expr)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let now = SystemTime::now();

        let next = schedule.upcoming(Utc)
            .find(|dt| {
                let duration = dt.signed_duration_since(Utc::now());
                duration.num_seconds() >= 1
            })
            .ok_or_else(|| Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "No suitable upcoming execution time found"
            )))?;

        let duration = next.signed_duration_since(Utc::now());
        let scheduled_at = now.checked_add(Duration::from_secs(duration.num_seconds().max(1) as u64))
            .unwrap_or(now);

        Ok(Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::Recurring { cron_expr: cron_expr.to_string() },
            scheduled_at,
            payload,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: 3,
            metadata: None,
        })
    }

    pub fn create_next_occurrence(&self) -> Option<Self> {
        match &self.task_type {
            TaskType::OneShot => None,
            TaskType::Recurring { cron_expr } => {
                let schedule = Schedule::from_str(cron_expr).ok()?;
                let now = SystemTime::now();

                let next = schedule.upcoming(Utc)
                    .find(|dt| {
                        let duration = dt.signed_duration_since(Utc::now());
                        duration.num_seconds() >= 1
                    })?;

                let duration = next.signed_duration_since(Utc::now());
                let scheduled_at = now.checked_add(Duration::from_secs(duration.num_seconds().max(1) as u64))
                    .unwrap_or(now);

                Some(Self {
                    id: self.id,
                    name: self.name.clone(),
                    task_type: TaskType::Recurring { cron_expr: cron_expr.clone() },
                    scheduled_at,
                    payload: self.payload.clone(),
                    status: TaskStatus::Pending,
                    created_at: self.created_at,
                    updated_at: now,
                    retry_count: 0,
                    max_retries: self.max_retries,
                    metadata: self.metadata.clone(),
                })
            }
        }
    }

    pub fn update_status(&mut self, status: TaskStatus) {
        self.status = status;
        self.updated_at = SystemTime::now();
    }

    pub fn should_retry(&self) -> bool {
        matches!(self.status, TaskStatus::Failed(_)) &&
            self.retry_count < self.max_retries
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.updated_at = SystemTime::now();
    }
}