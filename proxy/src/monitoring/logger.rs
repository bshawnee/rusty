use tokio::sync::mpsc;
use tokio_rusqlite::Connection as AsyncConnection;
use tracing::{debug, error, info};
use std::time::Instant;

use crate::protocol::{QueryType, QueryStatus};
use pg_rusty_proxy_shared::DB_SCHEMA;

#[derive(Clone, Debug)]
pub struct QueryLogEntry {
    pub query_id: String,
    pub session_id: u64,
    pub backend_pid: Option<i32>,
    pub client_addr: std::net::SocketAddr,
    pub server_addr: std::net::SocketAddr,
    
    pub user_name: String,
    pub database_name: String,
    pub application_name: String,
    
    pub query_text: String,
    pub query_type: String,
    
    pub start_time: i64,
    pub execution_time_us: i64,
    
    pub status: String,
    pub error_message: Option<String>,
}

impl QueryLogEntry {
    pub fn new(
        session_id: u64,
        backend_pid: Option<i32>,
        client_addr: std::net::SocketAddr,
        server_addr: std::net::SocketAddr,
        user_name: String,
        database_name: String,
        application_name: String,
        query_text: String,
        query_type: QueryType,
        start_time: std::time::SystemTime,
        execution_time_ms: f64,
        status: QueryStatus,
    ) -> Self {
        use uuid::Uuid;
        
        let query_id = Uuid::now_v7().to_string();
        
        let start_time_us = start_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        
        let (status_str, error_message) = match status {
            QueryStatus::Success => ("success".to_string(), None),
            QueryStatus::Error(msg) => ("error".to_string(), Some(msg)),
            QueryStatus::InProgress => ("in_progress".to_string(), None),
        };
        
        Self {
            query_id,
            session_id,
            backend_pid,
            client_addr,
            server_addr,
            user_name,
            database_name,
            application_name,
            query_text,
            query_type: format!("{:?}", query_type),
            start_time: start_time_us,
            execution_time_us: (execution_time_ms * 1000.0) as i64,
            status: status_str,
            error_message,
        }
    }
}

pub struct LogWriter {
    db: AsyncConnection,
    rx: mpsc::Receiver<QueryLogEntry>,
    batch_size: usize,
    flush_interval: tokio::time::Duration,
}

impl LogWriter {
    pub async fn new(
        db_path: &str,
        rx: mpsc::Receiver<QueryLogEntry>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let db = AsyncConnection::open(db_path).await?;
        
        // Initialize schema
        db.call(|conn| {
            conn.execute_batch(DB_SCHEMA)?;
            
            // Optimize SQLite for write-heavy workload
            conn.execute_batch(r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA cache_size = -64000;  -- 64MB
                PRAGMA temp_store = MEMORY;
                PRAGMA mmap_size = 268435456; -- 256MB
                PRAGMA page_size = 4096;
            "#)?;
            
            Ok::<_, rusqlite::Error>(())
        }).await?;
        
        info!(db_path = %db_path, "SQLite logger initialized");
        
        Ok(Self {
            db,
            rx,
            batch_size: 100,
            flush_interval: tokio::time::Duration::from_millis(100),
        })
    }
    
    pub async fn run(mut self) {
        let mut buffer = Vec::with_capacity(self.batch_size);
        let mut flush_timer = tokio::time::interval(self.flush_interval);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        info!("LogWriter started");
        
        loop {
            tokio::select! {
                Some(entry) = self.rx.recv() => {
                    buffer.push(entry);
                    
                    if buffer.len() >= self.batch_size {
                        if let Err(e) = self.flush_batch(&mut buffer).await {
                            error!(error = %e, "Failed to flush batch");
                        }
                    }
                }
                
                _ = flush_timer.tick() => {
                    if !buffer.is_empty() {
                        if let Err(e) = self.flush_batch(&mut buffer).await {
                            error!(error = %e, "Failed to flush batch on timer");
                        }
                    }
                }
                
                else => {
                    // Channel closed
                    if !buffer.is_empty() {
                        if let Err(e) = self.flush_batch(&mut buffer).await {
                            error!(error = %e, "Failed to flush final batch");
                        }
                    }
                    info!("LogWriter shutting down");
                    break;
                }
            }
        }
    }
    
    async fn flush_batch(&self, buffer: &mut Vec<QueryLogEntry>) -> Result<(), Box<dyn std::error::Error>> {
        let entries = std::mem::take(buffer);
        let count = entries.len();
        
        let start = Instant::now();
        
        self.db.call(move |conn| {
            let tx = conn.transaction()?;
            
            {
                let mut stmt = tx.prepare_cached(r#"
                    INSERT INTO query_logs (
                        query_id, session_id, backend_pid, client_addr, server_addr,
                        user_name, database_name, application_name,
                        query_text, query_type,
                        start_time, execution_time_us,
                        status, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#)?;
                
                for entry in entries {
                    stmt.execute(rusqlite::params![
                        entry.query_id,
                        entry.session_id as i64,
                        entry.backend_pid,
                        entry.client_addr.to_string(),
                        entry.server_addr.to_string(),
                        entry.user_name,
                        entry.database_name,
                        entry.application_name,
                        entry.query_text,
                        entry.query_type,
                        entry.start_time,
                        entry.execution_time_us,
                        entry.status,
                        entry.error_message,
                    ])?;
                }
            }
            
            tx.commit()?;
            Ok::<_, rusqlite::Error>(())
        }).await?;
        
        let elapsed = start.elapsed();
        
        debug!(
            count = count,
            elapsed_ms = elapsed.as_millis(),
            throughput = count as f64 / elapsed.as_secs_f64(),
            "Flushed batch to SQLite"
        );
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_log_writer() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let (tx, rx) = mpsc::channel(100);
        let log_writer = LogWriter::new(db_path.to_str().unwrap(), rx)
            .await
            .unwrap();
        
        let handle = tokio::spawn(async move {
            log_writer.run().await;
        });
        
        // Send some log entries
        for i in 0..10 {
            let entry = QueryLogEntry::new(
                1,
                Some(12345),
                "127.0.0.1:5000".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:5432".parse::<SocketAddr>().unwrap(),
                "test".to_string(),
                "testdb".to_string(),
                "test_app".to_string(),
                format!("SELECT {}", i),
                QueryType::Select,
                std::time::SystemTime::now(),
                1.5,
                QueryStatus::Success,
            );
            
            tx.send(entry).await.unwrap();
        }
        
        // Drop sender to close channel
        drop(tx);
        
        // Wait for writer to finish
        handle.await.unwrap();
        
        // Verify data was written
        let db = AsyncConnection::open(db_path.to_str().unwrap()).await.unwrap();
        let count: i64 = db.call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM query_logs", [], |row| row.get(0))
        }).await.unwrap();
        
        assert_eq!(count, 10);
    }
}