use std::time::{Instant, SystemTime};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryStatus {
    Success,
    Error(String),
    InProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Transaction,
    Explain,
    Maintenance,  // VACUUM, REINDEX, etc.
    Copy,
    Session,      // SET, SHOW, etc.
    Other(String),
}

#[derive(Debug)]
pub struct PendingQuery {
    pub query_id: u64,
    pub query: String,
    pub query_type: QueryType,
    pub start_time: Instant,
}