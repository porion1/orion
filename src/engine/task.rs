use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::str::FromStr;
use cron::Schedule;
use chrono::Utc;
use std::collections::HashMap;

/// Task types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    OneShot,
    Recurring { cron_expr: String },
}

/// Task execution status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

/// Main Task struct
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
    pub metadata: Option<HashMap<String, String>>,
}

mod system_time_serde {
    use serde::{Serializer, Deserializer, Deserialize};
    use std::time::{SystemTime, Duration, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where D: Deserializer<'de> {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

impl Task {
    pub fn new_one_shot(
        name: &str,
        delay: Duration,
        payload: Option<serde_json::Value>,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::OneShot,
            scheduled_at: now + delay,
            payload,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: 3,
            metadata: None,
        }
    }

    pub fn new_recurring(
        name: &str,
        cron_expr: &str,
        payload: Option<serde_json::Value>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let schedule = Schedule::from_str(cron_expr)?;
        let now = SystemTime::now();

        let next = schedule.upcoming(Utc).next().ok_or("No next run")?;
        let delay = Duration::from_secs(
            next.signed_duration_since(Utc::now()).num_seconds().max(1) as u64
        );

        Ok(Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::Recurring { cron_expr: cron_expr.to_string() },
            scheduled_at: now + delay,
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
        let TaskType::Recurring { cron_expr } = &self.task_type else { return None };

        let schedule = Schedule::from_str(cron_expr).ok()?;
        let next = schedule.upcoming(Utc).next()?;
        let delay = Duration::from_secs(
            next.signed_duration_since(Utc::now()).num_seconds().max(1) as u64
        );

        let now = SystemTime::now();
        Some(Self {
            id: Uuid::new_v4(),
            name: self.name.clone(),
            task_type: self.task_type.clone(),
            scheduled_at: now + delay,
            payload: self.payload.clone(),
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: self.max_retries,
            metadata: self.metadata.clone(),
        })
    }

    pub fn update_status(&mut self, status: TaskStatus) {
        self.status = status;
        self.updated_at = SystemTime::now();
    }
}

/// ✅ FIXED: Task → QueueTask (task_type INCLUDED)
impl From<&Task> for crate::queue::QueueTask {
    fn from(task: &Task) -> Self {
        crate::queue::QueueTask {
            id: task.id,
            name: task.name.clone(),
            scheduled_at: task.scheduled_at,
            priority: crate::queue::TaskPriority::Medium,
            payload: task.payload.clone(),
            retry_count: task.retry_count,
            max_retries: task.max_retries,
            task_type: task.task_type.clone(), // ✅ FIX
        }
    }
}
