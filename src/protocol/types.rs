use std::time::{Instant, SystemTime};

#[derive(Debug, Clone)]
pub enum TrafficDirection {
    ClientToServer,
    ServerToClient,
}

#[derive(Debug)]
pub struct TrafficData {
    pub direction: TrafficDirection,
    pub client_addr: std::net::SocketAddr,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub user_name: String,
    pub database: String,
    pub application_name: String,
}

#[derive(Debug, Clone)]
pub enum QueryStatus {
    Success,
    Error(String),
    InProgress,
}

#[derive(Debug, Clone)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Transaction,
    Other(String),
}

#[derive(Debug, Clone)]
pub struct QueryLog {
    pub query_id: u64,
    pub query: String,
    pub status: QueryStatus,
    pub execution_time_ms: f64,
    pub query_type: QueryType,
    pub timestamp: SystemTime,
}

#[derive(Debug)]
pub struct PendingQuery {
    pub query_id: u64,
    pub query: String,
    pub query_type: QueryType,
    pub start_time: Instant,
}