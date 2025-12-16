use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use serde::Serialize;

#[derive(Clone, Debug)]
pub struct QueryExecution {
    pub query_id: u64,
    pub query: String,
    pub user: String,
    pub database: String,
    pub application_name: String,
    pub client_addr: SocketAddr,
    pub server_addr: SocketAddr,
    pub start_time: Instant,
    pub backend_pid: Option<i32>,
    pub secret_key: Option<i32>,
}

#[derive(Clone)]
pub struct ActiveQueries {
    queries: Arc<RwLock<HashMap<u64, QueryExecution>>>,
}

impl ActiveQueries {
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn start_query(&self, exec: QueryExecution) {
        let mut queries = self.queries.write().await;
        queries.insert(exec.query_id, exec);
    }
    
    pub async fn finish_query(&self, query_id: u64) {
        let mut queries = self.queries.write().await;
        queries.remove(&query_id);
    }
    
    pub async fn get_query(&self, query_id: u64) -> Option<QueryExecution> {
        let queries = self.queries.read().await;
        queries.get(&query_id).cloned()
    }

    pub async fn get_top_running(&self, limit: usize) -> Vec<QueryExecution> {
        let queries = self.queries.read().await;
        let mut active: Vec<_> = queries.values().cloned().collect();
        
        active.sort_by(|a, b| {
            b.start_time.elapsed().cmp(&a.start_time.elapsed())
        });
        
        active.into_iter().take(limit).collect()
    }
}

#[derive(Clone, Serialize)]
pub struct ActiveQueryInfo {
    pub query_id: u64,
    pub query: String,
    pub user: String,
    pub database: String,
    pub application_name: String,
    pub client_addr: String,
    pub elapsed_seconds: f64,
}

impl From<QueryExecution> for ActiveQueryInfo {
    fn from(q: QueryExecution) -> Self {
        Self {
            query_id: q.query_id,
            query: q.query,
            user: q.user,
            database: q.database,
            application_name: q.application_name,
            client_addr: q.client_addr.to_string(),
            elapsed_seconds: q.start_time.elapsed().as_secs_f64(),
        }
    }
}