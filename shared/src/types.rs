use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuerySnapshot {
    pub query_id: u64,
    pub backend_pid: i32,
    pub secret_key: i32,
    pub start_time_us: u64,
    
    pub query: String,
    pub user: String,
    pub database: String,
    pub application_name: String,
    pub client_addr: String,
    pub server_addr: String,
}

impl QuerySnapshot {
    pub fn elapsed_seconds(&self) -> f64 {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        (now_us - self.start_time_us) as f64 / 1_000_000.0
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventType {
    Started,
    Finished,
    Error,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Event {
    pub query_id: u64,
    pub event_type: EventType,
    pub timestamp_us: u64,
    pub execution_time_us: u64,
}