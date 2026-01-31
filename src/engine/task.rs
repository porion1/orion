use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use cron::Schedule;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

// FIXED: Changed from crate::queue to super::queue (relative import)
use super::queue::{QueueTask, TaskPriority};

/// --------------------------------
/// Task Error
/// --------------------------------
#[derive(Debug, Error)]
pub enum TaskError {
    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("Cron produced no future dates")]
    NoFutureDates,
}

/// --------------------------------
/// Task Type
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskType {
    OneShot,
    Recurring {
        cron_expr: String,
    },
}

/// --------------------------------
/// Task Status
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

/// --------------------------------
/// Task Model (Source of Truth)
/// --------------------------------
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

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Task {}

/// --------------------------------
/// SystemTime <-> millis serde
/// --------------------------------
mod system_time_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        serializer.serialize_u64(millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

/// --------------------------------
/// Constructors
/// --------------------------------
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
    ) -> Result<Self, TaskError> {
        // Validate cron expression first
        let schedule = Schedule::from_str(cron_expr)
            .map_err(|e| TaskError::InvalidCron(e.to_string()))?;

        // Get next occurrence
        let next = schedule
            .upcoming(Utc)
            .next()
            .ok_or(TaskError::NoFutureDates)?;

        let now = SystemTime::now();
        let delay_secs = next
            .signed_duration_since(Utc::now())
            .num_seconds()
            .max(1) as u64;

        Ok(Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::Recurring {
                cron_expr: cron_expr.to_string(),
            },
            scheduled_at: now + Duration::from_secs(delay_secs),
            payload,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: 3,
            metadata: None,
        })
    }

    /// --------------------------------
    /// Recurring scheduling (PURE)
    /// --------------------------------
    pub fn create_next_occurrence(&self) -> Option<Self> {
        let TaskType::Recurring { cron_expr } = &self.task_type else {
            return None;
        };

        let schedule = Schedule::from_str(cron_expr).ok()?;
        let next = schedule.upcoming(Utc).next()?;

        let now = SystemTime::now();
        let delay_secs = next
            .signed_duration_since(Utc::now())
            .num_seconds()
            .max(1) as u64;

        Some(Self {
            id: Uuid::new_v4(),
            name: self.name.clone(),
            task_type: self.task_type.clone(),
            scheduled_at: now + Duration::from_secs(delay_secs),
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

    /// Helper to check if task can be retried
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Helper to get delay until scheduled execution
    pub fn delay_until_scheduled(&self) -> Option<Duration> {
        self.scheduled_at
            .duration_since(SystemTime::now())
            .ok()
    }

    /// Helper to check if task is ready to execute
    pub fn is_ready(&self) -> bool {
        matches!(self.status, TaskStatus::Pending) &&
            self.scheduled_at <= SystemTime::now()
    }

    /// --------------------------------
    /// QueueTask → Task (scheduler fix)
    /// --------------------------------
    pub fn from_queue_task(qt: &QueueTask) -> Self {
        let now = SystemTime::now();

        Self {
            id: qt.id,
            name: qt.name.clone(),
            task_type: qt.task_type.clone(),
            scheduled_at: qt.scheduled_at,
            payload: qt.payload.clone(),
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: qt.retry_count,
            max_retries: qt.max_retries,
            metadata: None,
        }
    }

    /// Convert to QueueTask with proper priority calculation
    pub fn to_queue_task(&self, priority: TaskPriority) -> QueueTask {
        QueueTask {
            id: self.id,
            name: self.name.clone(),
            scheduled_at: self.scheduled_at,
            priority,
            payload: self.payload.clone(),
            retry_count: self.retry_count,
            max_retries: self.max_retries,
            task_type: self.task_type.clone(),
        }
    }
}

/// --------------------------------
/// Task → QueueTask (explicit)
/// --------------------------------
impl From<&Task> for QueueTask {
    fn from(task: &Task) -> Self {
        task.to_queue_task(TaskPriority::Medium)
    }
}

/// --------------------------------
/// From<Task> for QueueTask
/// --------------------------------
impl From<Task> for QueueTask {
    fn from(task: Task) -> Self {
        (&task).into()
    }
}