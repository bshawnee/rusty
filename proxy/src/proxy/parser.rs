use bytes::{Buf, BytesMut};
use postgres_protocol::message::backend::{Message, self as backend};
use fallible_iterator::FallibleIterator;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use std::net::SocketAddr;
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pg_rusty_proxy_shared::{ProxyRocksDB, QuerySnapshot, EventType};
use crate::protocol::{
    ConnectionInfo, QueryType, QueryStatus,
    PendingQuery, parse_cstring, detect_query_type,
};
use crate::monitoring::QueryLogEntry;

static QUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct PostgresParser {
    client_addr: SocketAddr,
    server_addr: SocketAddr,
    client_buffer: BytesMut,
    server_buffer: BytesMut,
    startup_received: bool,
    
    connection_info: Option<ConnectionInfo>,
    pending_query: Option<PendingQuery>,
    
    backend_pid: Option<i32>,
    secret_key: Option<i32>,
    
    rocksdb: Arc<ProxyRocksDB>,
    session_id: u64,
    log_tx: mpsc::Sender<QueryLogEntry>,
}

impl PostgresParser {
    pub fn new(
        client_addr: SocketAddr,
        server_addr: SocketAddr,
        rocksdb: Arc<ProxyRocksDB>,
        session_id: u64,
        log_tx: mpsc::Sender<QueryLogEntry>,
    ) -> Self {
        Self {
            client_addr,
            server_addr,
            client_buffer: BytesMut::with_capacity(8192),
            server_buffer: BytesMut::with_capacity(8192),
            startup_received: false,
            connection_info: None,
            pending_query: None,
            backend_pid: None,
            secret_key: None,
            rocksdb,
            session_id,
            log_tx,
        }
    }
    
    pub fn parse_client_message(&mut self, data: &[u8]) {
        self.client_buffer.extend_from_slice(data);
        
        // Startup message handling
        loop {
            if !self.startup_received {
                if self.client_buffer.len() < 8 {
                    return;
                }
                
                let msg_len = u32::from_be_bytes([
                    self.client_buffer[0],
                    self.client_buffer[1],
                    self.client_buffer[2],
                    self.client_buffer[3],
                ]) as usize;
                
                if self.client_buffer.len() < msg_len {
                    return;
                }
                
                let startup_data = self.client_buffer.split_to(msg_len);
                self.handle_startup_message(&startup_data);
                
                if msg_len == 8 {
                    self.startup_received = false;
                } else {
                    self.startup_received = true;
                    break;
                }
            } else {
                break;
            }
        }
        
        // Regular messages
        while self.client_buffer.len() >= 5 {
            let msg_type = self.client_buffer[0];
            
            let msg_len = u32::from_be_bytes([
                self.client_buffer[1],
                self.client_buffer[2],
                self.client_buffer[3],
                self.client_buffer[4],
            ]) as usize;
            
            let total_len = 1 + msg_len;
            
            if self.client_buffer.len() < total_len {
                break;
            }
            
            let msg_data = self.client_buffer.split_to(total_len);
            let body = &msg_data[5..];
            
            self.handle_client_message(msg_type, body);
        }
    }
    
    pub fn parse_server_message(&mut self, data: &[u8]) {
        self.server_buffer.extend_from_slice(data);
        
        // SSL response handling
        if self.server_buffer.len() == 1 {
            let byte = self.server_buffer[0];
            if byte == b'N' || byte == b'S' {
                match byte {
                    b'N' => debug!(client = %self.client_addr, "Server: SSL not supported"),
                    b'S' => debug!(client = %self.client_addr, "Server: SSL accepted"),
                    _ => {}
                }
                self.server_buffer.advance(1);
                return;
            }
        }
        
        // Parse backend messages
        loop {
            match backend::Message::parse(&mut self.server_buffer) {
                Ok(Some(message)) => {
                    self.handle_server_message_parsed(message);
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    error!(
                        client = %self.client_addr,
                        error = ?e,
                        "Failed to parse server message"
                    );
                    self.server_buffer.clear();
                    break;
                }
            }
        }
    }
    
    fn handle_startup_message(&mut self, data: &[u8]) {
        if data.len() < 8 {
            return;
        }
        
        let mut protocol_bytes = &data[4..8];
        let protocol_version = protocol_bytes.get_u32();
        
        match protocol_version {
            196608 => {
                debug!(
                    client = %self.client_addr,
                    protocol = "3.0",
                    "Startup message"
                );
                
                let mut user_name = String::new();
                let mut database = String::new();
                let mut application_name = String::new();
                
                let mut pos = 8;
                while pos < data.len() - 1 {
                    if let Some((key, rest)) = parse_cstring(&data[pos..]) {
                        if key.is_empty() {
                            break;
                        }
                        if let Some((value, _)) = parse_cstring(rest) {
                            match key {
                                "user" => user_name = value.to_string(),
                                "database" => database = value.to_string(),
                                "application_name" => application_name = value.to_string(),
                                _ => {}
                            }
                            
                            debug!(
                                client = %self.client_addr,
                                param_key = %key,
                                param_value = %value,
                                "Startup parameter"
                            );
                            pos += key.len() + 1 + value.len() + 1;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                
                self.connection_info = Some(ConnectionInfo {
                    user_name,
                    database,
                    application_name,
                });
                
                if let Some(ref conn) = self.connection_info {
                    info!(
                        client = %self.client_addr,
                        session_id = self.session_id,
                        user = %conn.user_name,
                        database = %conn.database,
                        app = %conn.application_name,
                        "Connection established"
                    );
                }
            }
            80877103 => {
                debug!(client = %self.client_addr, "SSL request");
            }
            80877102 => {
                debug!(client = %self.client_addr, "Cancel request");
            }
            _ => {
                debug!(
                    client = %self.client_addr,
                    protocol = protocol_version,
                    "Unknown protocol version"
                );
            }
        }
    }
    
    fn handle_client_message(&mut self, msg_type: u8, body: &[u8]) {
        match msg_type {
            b'Q' => self.handle_simple_query(body),
            b'P' => self.handle_parse(body),
            b'B' => self.handle_bind(body),
            b'E' => self.handle_execute(body),
            b'D' => self.handle_describe(body),
            b'S' => debug!(client = %self.client_addr, "Sync"),
            b'X' => info!(client = %self.client_addr, "Client terminating connection"),
            b'p' => debug!(client = %self.client_addr, "Password sent"),
            _ => {
                debug!(
                    client = %self.client_addr,
                    msg_type = ?(msg_type as char),
                    body_len = body.len(),
                    "Other client message"
                );
            }
        }
    }
    
    fn handle_simple_query(&mut self, body: &[u8]) {
        if let Ok(query) = std::str::from_utf8(body) {
            let query = query.trim_end_matches('\0');
            let query_type = detect_query_type(query);
            
            self.start_query(query, query_type);
        }
    }
    
    fn handle_parse(&mut self, body: &[u8]) {
        if let Some((statement_name, rest)) = parse_cstring(body) {
            if let Some((query, _params)) = parse_cstring(rest) {
                let query_type = detect_query_type(query);
                self.start_query(query, query_type);
                
                debug!(
                    client = %self.client_addr,
                    statement = %statement_name,
                    "Prepared Statement Parse"
                );
            }
        }
    }
    
    fn handle_bind(&mut self, body: &[u8]) {
        if let Some((portal_name, rest)) = parse_cstring(body) {
            if let Some((statement_name, _rest)) = parse_cstring(rest) {
                debug!(
                    client = %self.client_addr,
                    portal = %portal_name,
                    statement = %statement_name,
                    "Bind parameters"
                );
            }
        }
    }
    
    fn handle_execute(&mut self, body: &[u8]) {
        if let Some((portal_name, rest)) = parse_cstring(body) {
            if rest.len() >= 4 {
                let max_rows = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
                debug!(
                    client = %self.client_addr,
                    portal = %portal_name,
                    max_rows = max_rows,
                    "Execute portal"
                );
            }
        }
    }
    
    fn handle_describe(&mut self, body: &[u8]) {
        if body.len() > 0 {
            let desc_type = body[0] as char;
            if let Some((name, _)) = parse_cstring(&body[1..]) {
                debug!(
                    client = %self.client_addr,
                    desc_type = ?desc_type,
                    name = %name,
                    "Describe"
                );
            }
        }
    }
    
    fn start_query(&mut self, query: &str, query_type: QueryType) {
        let query_id = QUERY_COUNTER.fetch_add(1, Ordering::SeqCst);
        
        let snapshot = QuerySnapshot {
            query_id,
            backend_pid: self.backend_pid.unwrap_or(0),
            secret_key: self.secret_key.unwrap_or(0),
            start_time_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            query: query.to_string(),
            user: self.connection_info.as_ref()
                .map(|c| c.user_name.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            database: self.connection_info.as_ref()
                .map(|c| c.database.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            application_name: self.connection_info.as_ref()
                .map(|c| c.application_name.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            client_addr: self.client_addr.to_string(),
            server_addr: self.server_addr.to_string(),
        };
        
        // Add to RocksDB
        let rocksdb = self.rocksdb.clone();
        let snapshot_clone = snapshot.clone();
        tokio::spawn(async move {
            if let Err(e) = rocksdb.add_active_query(&snapshot_clone) {
                error!(
                    error = %e,
                    query_id = snapshot_clone.query_id,
                    "Failed to add active query to RocksDB"
                );
            }
        });
        
        self.pending_query = Some(PendingQuery {
            query_id,
            query: query.to_string(),
            query_type: query_type.clone(),
            start_time: Instant::now(),
        });
        
        info!(
            client = %self.client_addr,
            session_id = self.session_id,
            query_id = query_id,
            query_type = ?query_type,
            query = %query,
            "SQL Query started"
        );
    }
    
    fn handle_server_message_parsed(&mut self, message: Message) {
        match message {
            Message::CommandComplete(body) => {
                if let Ok(tag) = body.tag() {
                    self.finish_query(QueryStatus::Success);
                    debug!(
                        client = %self.client_addr,
                        tag = %tag,
                        "CommandComplete"
                    );
                }
            }
            
            Message::ErrorResponse(err_body) => {
                let mut severity = String::new();
                let mut message = String::new();
                let mut sqlstate = String::new();
                
                let mut fields_iter = err_body.fields();
                while let Ok(Some(field)) = fields_iter.next() {
                    match field.type_() {
                        b'S' => {
                            if let Ok(s) = std::str::from_utf8(field.value_bytes()) {
                                severity = s.to_string();
                            }
                        }
                        b'M' => {
                            if let Ok(m) = std::str::from_utf8(field.value_bytes()) {
                                message = m.to_string();
                            }
                        }
                        b'C' => {
                            if let Ok(c) = std::str::from_utf8(field.value_bytes()) {
                                sqlstate = c.to_string();
                            }
                        }
                        _ => {}
                    }
                }
                
                self.finish_query(QueryStatus::Error(message.clone()));
                
                error!(
                    client = %self.client_addr,
                    severity = %severity,
                    sqlstate = %sqlstate,
                    message = %message,
                    "PostgreSQL Error"
                );
            }
            
            Message::BackendKeyData(body) => {
                self.backend_pid = Some(body.process_id());
                self.secret_key = Some(body.secret_key());
                
                debug!(
                    client = %self.client_addr,
                    process_id = body.process_id(),
                    secret_key = body.secret_key(),
                    "BackendKeyData"
                );
            }
            
            Message::ReadyForQuery(body) => {
                let status = match body.status() {
                    b'I' => "Idle",
                    b'T' => "Transaction",
                    b'E' => "Failed Transaction",
                    _ => "Unknown",
                };
                debug!(
                    client = %self.client_addr,
                    status = status,
                    "ReadyForQuery"
                );
            }
            
            _ => {
                // Other messages
            }
        }
    }
    
    fn finish_query(&mut self, status: QueryStatus) {
        if let Some(pending) = self.pending_query.take() {
            let execution_time_us = pending.start_time.elapsed().as_micros() as u64;
            let is_error = matches!(status, QueryStatus::Error(_));
            
            // Remove from RocksDB
            let rocksdb = self.rocksdb.clone();
            let query_id = pending.query_id;
            tokio::spawn(async move {
                if let Err(e) = rocksdb.remove_active_query(query_id, execution_time_us, is_error) {
                    error!(
                        error = %e,
                        query_id = query_id,
                        "Failed to remove active query from RocksDB"
                    );
                }
            });
            
            // Log to SQLite (batched)
            if let Some(ref conn) = self.connection_info {
                let entry = QueryLogEntry::new(
                    self.session_id,
                    self.backend_pid,
                    self.client_addr,
                    self.server_addr,
                    conn.user_name.clone(),
                    conn.database.clone(),
                    conn.application_name.clone(),
                    pending.query.clone(),
                    pending.query_type.clone(),
                    std::time::SystemTime::now() - 
                        std::time::Duration::from_micros(execution_time_us),
                    execution_time_us as f64 / 1000.0,
                    status,
                );
                
                let log_tx = self.log_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = log_tx.send(entry).await {
                        error!(error = %e, "Failed to send log entry");
                    }
                });
            }
            
            info!(
                client = %self.client_addr,
                query_id = query_id,
                execution_time_ms = execution_time_us as f64 / 1000.0,
                is_error = is_error,
                "SQL Query finished"
            );
        }
    }
}