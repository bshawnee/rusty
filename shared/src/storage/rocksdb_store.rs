use rocksdb::{DB, Options, ColumnFamilyDescriptor, IteratorMode, Direction};
use std::path::Path;
use std::sync::Arc;
use bincode::{serialize, deserialize};
use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};

use super::error::{Result, RocksDBError};
use crate::types::{QuerySnapshot, Event, EventType};

pub struct ProxyRocksDB {
    db: Arc<DB>,
}

impl ProxyRocksDB {
    /// Создать/открыть RocksDB (для proxy - read-write)
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        // Оптимизация для write-heavy workload
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        opts.set_max_write_buffer_number(3);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_max_background_jobs(4);
        
        // Compression
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        
        // WAL
        opts.set_manual_wal_flush(false);
        
        // Column families
        let cfs = vec![
            ColumnFamilyDescriptor::new("active_queries", Options::default()),
            ColumnFamilyDescriptor::new("events", Options::default()),
            ColumnFamilyDescriptor::new("metrics", Options::default()),
            ColumnFamilyDescriptor::new("blocking", Options::default()),
        ];
        
        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        
        Ok(Self {
            db: Arc::new(db),
        })
    }
    
    /// Открыть RocksDB в read-only режиме (для API)
    pub fn open_readonly<P: AsRef<Path>>(path: P) -> Result<Self> {
        let opts = Options::default();
        
        let db = DB::open_cf_for_read_only(
            &opts,
            path,
            vec!["active_queries", "events", "metrics", "blocking"],
            false, // error_if_wal_file_exists
        )?;
        
        Ok(Self {
            db: Arc::new(db),
        })
    }
    
    // ==================== Active Queries ====================
    
    /// Добавить активный запрос
    pub fn add_active_query(&self, snapshot: &QuerySnapshot) -> Result<()> {
        let cf = self.db.cf_handle("active_queries")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let key = snapshot.query_id.to_be_bytes();
        let value = serialize(snapshot)?;
        
        self.db.put_cf(cf, key, value)?;
        
        // Также добавляем event
        self.add_event(Event {
            query_id: snapshot.query_id,
            event_type: EventType::Started,
            timestamp_us: snapshot.start_time_us,
            execution_time_us: 0,
        })?;
        
        Ok(())
    }
    
    /// Удалить активный запрос
    pub fn remove_active_query(
        &self,
        query_id: u64,
        execution_time_us: u64,
        is_error: bool,
    ) -> Result<()> {
        let cf = self.db.cf_handle("active_queries")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let key = query_id.to_be_bytes();
        self.db.delete_cf(cf, key)?;
        
        self.add_event(Event {
            query_id,
            event_type: if is_error { EventType::Error } else { EventType::Finished },
            timestamp_us: current_time_us(),
            execution_time_us,
        })?;
        
        Ok(())
    }
    
    /// Получить один активный запрос
    pub fn get_active_query(&self, query_id: u64) -> Result<Option<QuerySnapshot>> {
        let cf = self.db.cf_handle("active_queries")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let key = query_id.to_be_bytes();
        
        match self.db.get_cf(cf, key)? {
            Some(value) => {
                let snapshot = deserialize(&value)?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }
    
    /// Получить все активные запросы
    pub fn get_active_queries(&self) -> Result<Vec<QuerySnapshot>> {
        let cf = self.db.cf_handle("active_queries")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let mut queries = Vec::new();
        
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        
        for item in iter {
            let (_, value) = item?;
            let snapshot: QuerySnapshot = deserialize(&value)?;
            queries.push(snapshot);
        }
        
        Ok(queries)
    }
    
    // ==================== Events ====================
    
    /// Добавить событие (append-only log)
    pub fn add_event(&self, event: Event) -> Result<()> {
        let cf = self.db.cf_handle("events")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        // Key: timestamp_us (20 digits) + query_id (20 digits) - для сортировки
        let key = format!("{:020}_{:020}", event.timestamp_us, event.query_id);
        let value = serialize(&event)?;
        
        self.db.put_cf(cf, key.as_bytes(), value)?;
        Ok(())
    }
    
    /// Получить события начиная с timestamp
    pub fn get_events_since(&self, since_us: u64) -> Result<Vec<Event>> {
        let cf = self.db.cf_handle("events")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let start_key = format!("{:020}_", since_us);
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(start_key.as_bytes(), Direction::Forward)
        );
        
        let mut events = Vec::new();
        
        for item in iter {
            let (_, value) = item?;
            let event: Event = deserialize(&value)?;
            events.push(event);
        }
        
        Ok(events)
    }
    
    /// Получить события за диапазон
    pub fn get_events_range(&self, from_us: u64, to_us: u64) -> Result<Vec<Event>> {
        let cf = self.db.cf_handle("events")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let start_key = format!("{:020}_", from_us);
        let end_key = format!("{:020}_", to_us);
        
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(start_key.as_bytes(), Direction::Forward)
        );
        
        let mut events = Vec::new();
        
        for item in iter {
            let (key, value) = item?;
            
            if key.as_ref() >= end_key.as_bytes() {
                break;
            }
            
            let event: Event = deserialize(&value)?;
            events.push(event);
        }
        
        Ok(events)
    }
    
    /// Удалить старые события (compaction)
    pub fn compact_events(&self, retain_duration: std::time::Duration) -> Result<usize> {
        let cf = self.db.cf_handle("events")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let cutoff_us = current_time_us() - retain_duration.as_micros() as u64;
        let cutoff_key = format!("{:020}_", cutoff_us);
        
        let mut batch = rocksdb::WriteBatch::default();
        let mut deleted = 0;
        
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        
        for item in iter {
            let (key, _) = item?;
            
            if key.as_ref() < cutoff_key.as_bytes() {
                batch.delete_cf(cf, key);
                deleted += 1;
            } else {
                break; // Events отсортированы
            }
        }
        
        self.db.write(batch)?;
        Ok(deleted)
    }
    
    // ==================== Metrics ====================
    
    /// Increment counter
    pub fn increment_metric(&self, name: &str, delta: u64) -> Result<u64> {
        let cf = self.db.cf_handle("metrics")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let current = match self.db.get_cf(cf, name.as_bytes())? {
            Some(bytes) => u64::from_be_bytes(bytes.as_ref().try_into().unwrap_or([0; 8])),
            None => 0,
        };
        
        let new_value = current + delta;
        self.db.put_cf(cf, name.as_bytes(), new_value.to_be_bytes())?;
        
        Ok(new_value)
    }
    
    /// Get metric value
    pub fn get_metric(&self, name: &str) -> Result<u64> {
        let cf = self.db.cf_handle("metrics")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        match self.db.get_cf(cf, name.as_bytes())? {
            Some(bytes) => {
                let value = u64::from_be_bytes(bytes.as_ref().try_into().unwrap_or([0; 8]));
                Ok(value)
            }
            None => Ok(0),
        }
    }
    
    /// Get all metrics
    pub fn get_all_metrics(&self) -> Result<std::collections::HashMap<String, u64>> {
        let cf = self.db.cf_handle("metrics")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let mut metrics = std::collections::HashMap::new();
        
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item?;
            let name = String::from_utf8_lossy(&key).to_string();
            let count = u64::from_be_bytes(value.as_ref().try_into().unwrap_or([0; 8]));
            metrics.insert(name, count);
        }
        
        Ok(metrics)
    }
    
    // ==================== Blocking Info ====================
    
    /// Сохранить информацию о блокировке
    pub fn save_blocking_info(&self, query_id: u64, blocking_info: &serde_json::Value) -> Result<()> {
        let cf = self.db.cf_handle("blocking")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let key = query_id.to_be_bytes();
        let value = serde_json::to_vec(blocking_info)
            .map_err(|e| RocksDBError::InvalidData(e.to_string()))?;
        
        self.db.put_cf(cf, key, value)?;
        Ok(())
    }
    
    /// Получить информацию о блокировке
    pub fn get_blocking_info(&self, query_id: u64) -> Result<Option<serde_json::Value>> {
        let cf = self.db.cf_handle("blocking")
            .ok_or_else(|| RocksDBError::InvalidData("CF not found".into()))?;
        
        let key = query_id.to_be_bytes();
        
        match self.db.get_cf(cf, key)? {
            Some(value) => {
                let info = serde_json::from_slice(&value)
                    .map_err(|e| RocksDBError::InvalidData(e.to_string()))?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }
}

// Helper function
fn current_time_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_active_queries() {
        let temp_dir = TempDir::new().unwrap();
        let db = ProxyRocksDB::open(temp_dir.path()).unwrap();
        
        let snapshot = QuerySnapshot {
            query_id: 1,
            backend_pid: 12345,
            secret_key: 67890,
            start_time_us: current_time_us(),
            query: "SELECT 1".to_string(),
            user: "test".to_string(),
            database: "testdb".to_string(),
            application_name: "test_app".to_string(),
            client_addr: "127.0.0.1:5000".to_string(),
            server_addr: "127.0.0.1:5432".to_string(),
        };
        
        db.add_active_query(&snapshot).unwrap();
        
        let queries = db.get_active_queries().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].query_id, 1);
        
        db.remove_active_query(1, 1000, false).unwrap();
        
        let queries = db.get_active_queries().unwrap();
        assert_eq!(queries.len(), 0);
    }
    
    #[test]
    fn test_events() {
        let temp_dir = TempDir::new().unwrap();
        let db = ProxyRocksDB::open(temp_dir.path()).unwrap();
        
        let event = Event {
            query_id: 1,
            event_type: EventType::Started,
            timestamp_us: current_time_us(),
            execution_time_us: 0,
        };
        
        db.add_event(event).unwrap();
        
        let events = db.get_events_since(0).unwrap();
        assert_eq!(events.len(), 1);
    }
}