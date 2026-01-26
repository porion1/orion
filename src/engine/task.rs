use std::time::{Duration, Instant};
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

#[derive(Debug, Clone)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub task_type: TaskType,
    pub scheduled_at: Instant,
    pub payload: Option<serde_json::Value>,
    pub status: TaskStatus,
    pub created_at: Instant,
    pub updated_at: Instant,
    pub retry_count: u32,
    pub max_retries: u32,
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

impl Task {
    pub fn new_one_shot(name: &str, delay: Duration, payload: Option<serde_json::Value>) -> Self {
        let now = Instant::now();
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

    pub fn new_recurring(name: &str, cron_expr: &str, payload: Option<serde_json::Value>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Validate cron expression
        let schedule = Schedule::from_str(cron_expr)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let now = Instant::now();

        // Get the next occurrence that's at least 1 second in the future
        let next = schedule.upcoming(Utc)
            .find(|dt| {
                let duration = dt.signed_duration_since(Utc::now());
                duration.num_seconds() >= 1
            })
            .ok_or_else(|| Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "No suitable upcoming execution time found (at least 1 second in future)"
            )) as Box<dyn std::error::Error + Send + Sync>)?;

        let duration = next.signed_duration_since(Utc::now());
        let scheduled_at = now + Duration::from_secs(duration.num_seconds().max(1) as u64);

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
                let now = Instant::now();

                // Get the next occurrence that's at least 1 second in the future
                let next = schedule.upcoming(Utc)
                    .find(|dt| {
                        let duration = dt.signed_duration_since(Utc::now());
                        duration.num_seconds() >= 1
                    })?;

                let duration = next.signed_duration_since(Utc::now());
                let scheduled_at = now + Duration::from_secs(duration.num_seconds().max(1) as u64);

                Some(Self {
                    id: self.id, // Keep the same ID
                    name: self.name.clone(),
                    task_type: TaskType::Recurring { cron_expr: cron_expr.clone() },
                    scheduled_at,
                    payload: self.payload.clone(),
                    status: TaskStatus::Pending,
                    created_at: self.created_at,
                    updated_at: now,
                    retry_count: 0, // Reset retry count for next occurrence
                    max_retries: self.max_retries,
                    metadata: self.metadata.clone(),
                })
            }
        }
    }

    pub fn update_status(&mut self, status: TaskStatus) {
        self.status = status;
        self.updated_at = Instant::now();
    }

    pub fn should_retry(&self) -> bool {
        matches!(self.status, TaskStatus::Failed(_)) &&
            self.retry_count < self.max_retries
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.updated_at = Instant::now();
    }
}