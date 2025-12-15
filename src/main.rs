
use axum::routing::post;
use clap::Parser;
use pg_rusty_proxy::{TrafficData, TrafficDirection, cli};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tracing_subscriber;
use std::process;
use bytes::{Buf, BytesMut};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::backend;
use std::time::Instant;
use fallible_iterator::FallibleIterator;
use chrono;

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::Json;

use rusqlite::Connection;
use tokio_rusqlite::Connection as AsyncConnection;
use uuid::Uuid;

static QUERY_COUNTER_ID: AtomicU64 = AtomicU64::new(0);
use tokio_postgres::{NoTls, Client, Error as PgError};


const DB_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS query_logs (
    -- Уникальный ID (UUID v7 - timestamped, sortable)
    query_id TEXT PRIMARY KEY,
    
    -- Идентификаторы сессии/соединения
    session_id INTEGER NOT NULL,
    backend_pid INTEGER,
    client_addr TEXT NOT NULL,
    server_addr TEXT NOT NULL,
    
    -- Информация о подключении
    user_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    application_name TEXT,
    
    -- Сам запрос
    query_text TEXT NOT NULL,
    query_type TEXT NOT NULL,
    
    -- Время выполнения
    start_time INTEGER NOT NULL,  -- Unix timestamp в микросекундах
    execution_time_us INTEGER NOT NULL,  -- Микросекунды
    
    -- Статус
    status TEXT NOT NULL,  -- 'success', 'error', 'cancelled'
    error_message TEXT,
    
    -- Метаданные
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000000)
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_query_logs_backend_pid ON query_logs(backend_pid);
CREATE INDEX IF NOT EXISTS idx_query_logs_session_id ON query_logs(session_id);
CREATE INDEX IF NOT EXISTS idx_query_logs_start_time ON query_logs(start_time);
CREATE INDEX IF NOT EXISTS idx_query_logs_user ON query_logs(user_name);
CREATE INDEX IF NOT EXISTS idx_query_logs_status ON query_logs(status);

-- Таблица для активных транзакций
CREATE TABLE IF NOT EXISTS active_transactions (
    backend_pid INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    user_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    application_name TEXT,
    client_addr TEXT NOT NULL,
    server_addr TEXT NOT NULL,
    transaction_start INTEGER NOT NULL,  -- Unix timestamp в микросекундах
    last_query_id TEXT,
    last_update INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_active_tx_session ON active_transactions(session_id);
"#;


#[derive(Clone, Debug)]
struct QueryLogEntry {
    query_id: String,  // ✅ UUID v7
    session_id: u64,
    backend_pid: Option<i32>,
    client_addr: std::net::SocketAddr,
    server_addr: std::net::SocketAddr,
    
    user_name: String,
    database_name: String,
    application_name: String,
    
    query_text: String,
    query_type: String,
    
    start_time: i64,  // Unix timestamp в микросекундах
    execution_time_us: i64,
    
    status: String,
    error_message: Option<String>,
}

impl QueryLogEntry {
    fn new(
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
        // ✅ UUID v7 - timestamp-based, globally unique, sortable
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

struct QueryConterId {

}
impl QueryConterId {
    fn get_next() -> u64 {
        let counter = QUERY_COUNTER_ID.fetch_add(1, Ordering::SeqCst);
        counter
    }
}

macro_rules! fatal {
    ($err:expr, $msg:literal) => {{
        error!(fatal = true, error = %$err, $msg);
        process::exit(1);
    }};
    ($err:expr, $msg:literal, $($key:tt = $val:expr),+) => {{
        error!(fatal = true, error = %$err, $($key = %$val),+, $msg);
        process::exit(1);
    }};
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())  // ✅ INFO вместо DEBUG
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .with_line_number(false)
        .init();

    let cli_args = cli::CliArguments::parse();

    info!(
        downstream = %cli_args.downstream,
        upstream = %cli_args.upstream,
        "Starting proxy server"
    );

    // ✅ Создаем канал для логов (большой буфер)
    let (log_tx, log_rx) = mpsc::channel::<QueryLogEntry>(10000);

    // ✅ Запускаем LogWriter
    let log_writer = LogWriter::new("proxy_logs.db", log_rx)
        .await
        .unwrap_or_else(|e| fatal!(e, "Failed to initialize SQLite database"));
    
    tokio::spawn(async move {
        log_writer.run().await;
    });

    let active_queries = ActiveQueries::new();

    // ✅ HTTP сервер (передаем log_tx если нужен для transaction history)
    let active_queries_http = active_queries.clone();
    tokio::spawn(async move {
        start_http_server(active_queries_http).await;
    });

    let listener = TcpListener::bind(cli_args.downstream)
        .await
        .unwrap_or_else(|e|
            fatal!(e, "Failed to bind listener", addr = cli_args.downstream)
        );

    let mut session_counter = 0u64;

    loop {
        let (client_stream, client_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!(error = %e, "Failed to accept connection");
                continue;
            }
        };

        debug!(client = %client_addr, "New connection accepted");
        
        session_counter += 1;
        let session_id = session_counter;
        
        let upstream_addr = cli_args.upstream;
        let active_queries_clone = active_queries.clone();
        let log_tx_clone = log_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(
                client_stream,
                upstream_addr,
                client_addr,
                active_queries_clone,
                session_id,
                log_tx_clone,
            ).await {
                error!(client = %client_addr, error = %e, "Error handling client");
            }
        });
    }
}
// Парсит C-строку (заканчивается нулевым байтом)
fn parse_cstring(data: &[u8]) -> Option<(&str, &[u8])> {
    // Ищем null-байт
    let null_pos = data.iter().position(|&b| b == 0)?;
    
    // Извлекаем строку до null-байта
    let string = std::str::from_utf8(&data[..null_pos]).ok()?;
    
    // Возвращаем строку и остаток данных после null-байта
    let rest = &data[null_pos + 1..];
    
    Some((string, rest))
}



// Структура для хранения информации об ошибке
struct ErrorInfo {
    severity: String,
    message: String,
}

fn detect_query_type(query: &str) -> QueryType {
    // Приводим к lowercase и берем первое слово
    let query_lower = query.trim().to_lowercase();
    let first_word = query_lower.split_whitespace().next().unwrap_or("");
    
    match first_word {
        "select" | "with" => QueryType::Select,
        "insert" => QueryType::Insert,
        "update" => QueryType::Update,
        "delete" => QueryType::Delete,
        "create" => QueryType::Create,
        "drop" => QueryType::Drop,
        "alter" => QueryType::Alter,
        "begin" | "start" | "commit" | "rollback" | "savepoint" => QueryType::Transaction,
        _ => QueryType::Other(first_word.to_string()),
    }
}

// Структура для отслеживания состояния соединения
#[derive(Debug, Clone)]
struct ConnectionInfo {
    user_name: String,
    database: String,
    application_name: String,
}

// Информация о запросе для логирования
#[derive(Debug, Clone)]
struct QueryLog {
    query_id: u64,           // Наш внутренний ID
    query: String,           // Текст запроса
    status: QueryStatus,     // Успех/Ошибка
    execution_time_ms: f64,  // Время выполнения в миллисекундах
    query_type: QueryType,   // SELECT/INSERT/UPDATE и т.д.
    timestamp: std::time::SystemTime,  // Когда выполнен
}

#[derive(Debug, Clone)]
enum QueryStatus {
    Success,
    Error(String),  // Текст ошибки
    InProgress,
}

#[derive(Debug, Clone)]
enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Transaction,  // BEGIN/COMMIT/ROLLBACK
    Other(String),
}

// Структура для отслеживания выполняющегося запроса
struct PendingQuery {
    query_id: u64,
    query: String,
    query_type: QueryType,
    start_time: Instant,
}

struct PostgresParser {
    client_addr: std::net::SocketAddr,
    server_addr: std::net::SocketAddr,
    client_buffer: BytesMut,
    server_buffer: BytesMut,
    startup_received: bool,
    
    connection_info: Option<ConnectionInfo>,  // Заполняется при startup
    query_counter: u64,                        // Счетчик запросов
    pending_query: Option<PendingQuery>,       // Текущий выполняющийся запрос
    query_logs: Vec<QueryLog>,                 // История запросов

    backend_pid: Option<i32>,    // ✅ Добавили
    secret_key: Option<i32>, 
    active_queries: ActiveQueries,
    
    log_tx: mpsc::Sender<QueryLogEntry>,
    session_id: u64,
}

impl PostgresParser {
    fn new(
        client_addr: std::net::SocketAddr,
        server_addr: std::net::SocketAddr,
        active_queries: ActiveQueries,
        session_id: u64,
        log_tx: mpsc::Sender<QueryLogEntry>,) -> Self {
        Self {
            client_addr,
            server_addr,
            client_buffer: BytesMut::with_capacity(8192),
            server_buffer: BytesMut::with_capacity(8192),
            startup_received: false,
            connection_info: None,
            query_counter: 0,
            pending_query: None,
            query_logs: Vec::new(),
            backend_pid: None,
            secret_key: None,
            active_queries,
            session_id,
            log_tx
        }
    }
    
 fn parse_client_message(&mut self, data: &[u8]) {
    self.client_buffer.extend_from_slice(data);
    
    // Обрабатываем StartupMessage
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
            
            // ✅ Используем split_to для извлечения данных
            let startup_data = self.client_buffer.split_to(msg_len);
            
            self.handle_startup_message(&startup_data);
            
            if msg_len == 8 {
                self.startup_received = false;
            } else {
                self.startup_received = true;
                break;  // Переходим к обычным сообщениям
            }
        } else {
            break;
        }
    }
    
    // Обрабатываем обычные сообщения
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
        
        // ✅ Извлекаем сообщение целиком
        let msg_data = self.client_buffer.split_to(total_len);
        let body = &msg_data[5..];
        
        self.handle_client_message(msg_type, body);
    }
}
    
fn handle_startup_message(&mut self, data: &[u8]) {  // ✅ Изменили &self на &mut self
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
            
            // ✅ Парсим параметры и сохраняем
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
            
            // ✅ Сохраняем информацию о подключении
            self.connection_info = Some(ConnectionInfo {
                user_name,
                database,
                application_name,
            });
            
            info!(
                client = %self.client_addr,
                user = %self.connection_info.as_ref().unwrap().user_name,
                database = %self.connection_info.as_ref().unwrap().database,
                app = %self.connection_info.as_ref().unwrap().application_name,
                "Connection established"
            );
        }
        80877103 => {
            info!(client = %self.client_addr, "SSL request");
        }
        80877102 => {
            info!(client = %self.client_addr, "Cancel request");
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
    
    
fn parse_server_message(&mut self, data: &[u8]) {
    self.server_buffer.extend_from_slice(data);
    
    // Обработка SSL response (один байт 'N' или 'S')
    if self.server_buffer.len() == 1 {
        let byte = self.server_buffer[0];
        if byte == b'N' || byte == b'S' {
            match byte {
                b'N' => info!(client = %self.client_addr, "Server: SSL not supported"),
                b'S' => info!(client = %self.client_addr, "Server: SSL accepted"),
                _ => {}
            }
            self.server_buffer.advance(1);
            return;
        }
    }
    
    // ✅ Парсим сообщения через backend::Message::parse
    loop {
        match backend::Message::parse(&mut self.server_buffer) {
            Ok(Some(message)) => {
                // ✅ Получили полное сообщение, обрабатываем
                self.handle_server_message_parsed(message);
            }
            Ok(None) => {
                // Недостаточно данных для полного сообщения
                break;
            }
            Err(e) => {
                error!(
                    client = %self.client_addr,
                    error = ?e,
                    "Failed to parse server message"
                );
                // Очищаем буфер чтобы не зациклиться на битых данных
                self.server_buffer.clear();
                break;
            }
        }
    }
}

fn handle_client_message(&mut self, msg_type: u8, body: &[u8]) {  // ✅ Изменили &self на &mut self
    match msg_type {
        b'Q' => {
            // Query - простой SQL запрос
            if let Ok(query) = std::str::from_utf8(body) {
                let query = query.trim_end_matches('\0');
                let query_type = detect_query_type(query);
                
                self.query_counter = QueryConterId::get_next();

                let exec = QueryExecution {
                        query_id: self.query_counter,
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
                        client_addr: self.client_addr,
                        server_addr: self.server_addr,
                        start_time: Instant::now(),
                        backend_pid: self.backend_pid,
                        secret_key: self.secret_key,
                };
                let active_queries = self.active_queries.clone();
                    tokio::spawn(async move {
                        active_queries.start_query(exec).await;
                });
    
                // ✅ Создаем pending query
                self.pending_query = Some(PendingQuery {
                    query_id: self.query_counter,
                    query: query.to_string(),
                    query_type: query_type.clone(),
                    start_time: Instant::now(),
                });
                
                info!(
                    client = %self.client_addr,
                    query_id = self.query_counter,
                    query_type = ?query_type,
                    query = %query,
                    "SQL Query"
                );
            }
        }
        b'P' => {
            // Parse - подготовленный запрос
            if let Some((statement_name, rest)) = parse_cstring(body) {
                if let Some((query, _params)) = parse_cstring(rest) {
                    let query_type = detect_query_type(query);
                    
                    // ✅ Создаем pending query
                    self.query_counter = QueryConterId::get_next();

                    let exec = QueryExecution {
                        query_id: self.query_counter,
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
                        client_addr: self.client_addr,
                        server_addr: self.server_addr,
                        backend_pid: self.backend_pid,
                        secret_key: self.secret_key,
                        start_time: Instant::now(),
                    };
                        
                    let active_queries = self.active_queries.clone();
                    tokio::spawn(async move {
                        active_queries.start_query(exec).await;
                    });

                    self.pending_query = Some(PendingQuery {
                        query_id: self.query_counter,
                        query: query.to_string(),
                        query_type: query_type.clone(),
                        start_time: Instant::now(),
                    });
                    
                    info!(
                        client = %self.client_addr,
                        query_id = self.query_counter,
                        statement = %statement_name,
                        query_type = ?query_type,
                        query = %query,
                        "Prepared Statement Parse"
                    );
                }
            }
        }
        b'B' => {
            // Bind - привязка параметров
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
        b'E' => {
            // Execute - выполнение портала
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
        b'D' => {
            // Describe - запрос описания
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
        b'S' => {
            // Sync - синхронизация
            debug!(client = %self.client_addr, "Sync");
        }
        b'X' => {
            // Terminate - закрытие соединения
            info!(client = %self.client_addr, "Client terminating connection");
        }
        b'p' => {
            // PasswordMessage - пароль (не логируем!)
            debug!(client = %self.client_addr, "Password sent (hidden)");
        }
        _ => {
            // Другие типы сообщений
            debug!(
                client = %self.client_addr,
                msg_type = ?(msg_type as char),
                body_len = body.len(),
                "Other client message"
            );
        }
    }
}




fn handle_server_message_parsed(&mut self, message: Message) {
    match message {
        Message::CommandComplete(body) => {
            if let Ok(tag) = body.tag() {
                if let Some(pending) = self.pending_query.take() {
                    let execution_time = pending.start_time.elapsed().as_secs_f64() * 1000.0;
                    
                    // ✅ Создаем entry и отправляем в канал
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
                                std::time::Duration::from_secs_f64(execution_time / 1000.0),
                            execution_time,
                            QueryStatus::Success,
                        );
                        
                        // ✅ Асинхронная отправка (не блокируем!)
                        let log_tx = self.log_tx.clone();
                        tokio::spawn(async move {
                            if let Err(e) = log_tx.send(entry).await {
                                error!(error = %e, "Failed to send log entry");
                            }
                        });
                    }
                    
                    // Удаляем из активных запросов
                    let active_queries = self.active_queries.clone();
                    let query_id = pending.query_id;
                    tokio::spawn(async move {
                        active_queries.finish_query(query_id).await;
                    });
                }
                
                debug!(client = %self.client_addr, tag = %tag, "CommandComplete");
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

            if let Some(pending) = self.pending_query.take() {
                let execution_time = pending.start_time.elapsed().as_secs_f64() * 1000.0;

                // ✅ Отправляем в SQLite
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
                            std::time::Duration::from_secs_f64(execution_time / 1000.0),
                        execution_time,
                        QueryStatus::Error(message.clone()),
                    );

                    let log_tx = self.log_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = log_tx.send(entry).await {
                            error!(error = %e, "Failed to send log entry");
                        }
                    });
                }

                // Удаляем из активных запросов
                let active_queries = self.active_queries.clone();
                let query_id = pending.query_id;
                tokio::spawn(async move {
                    active_queries.finish_query(query_id).await;
                });
            }

            error!(
                client = %self.client_addr,
                severity = %severity,
                sqlstate = %sqlstate,
                message = %message,
                "PostgreSQL Error"
            );
        }

        Message::NoticeResponse(notice_body) => {
            let mut severity = String::new();
            let mut message = String::new();
            
            let mut fields_iter = notice_body.fields();
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
                    _ => {}
                }
            }
            
            debug!(
                client = %self.client_addr,
                severity = %severity,
                message = %message,
                "Notice"
            );
        }
        
        Message::ReadyForQuery(body) => {
            let status = match body.status() {
                b'I' => "Idle",
                b'T' => "Transaction",
                b'E' => "Failed Transaction",
                _ => "Unknown",
            };
            debug!(client = %self.client_addr, status = status, "ReadyForQuery");
        }
        
        Message::ParameterStatus(body) => {
            if let (Ok(name), Ok(value)) = (body.name(), body.value()) {
                debug!(
                    client = %self.client_addr,
                    param = %name,
                    value = %value,
                    "ParameterStatus"
                );
            }
        }
        
        Message::RowDescription(row_desc) => {
            // ✅ FallibleIterator для полей
            if let Ok(field_count) = row_desc.fields().count() {
                debug!(
                    client = %self.client_addr,
                    field_count = field_count,
                    "RowDescription"
                );
            }
        }
        
        Message::DataRow(_) => {
            // Не логируем каждую строку - слишком много
        }
        
        Message::ParseComplete => {
            debug!(client = %self.client_addr, "ParseComplete");
        }
        
        Message::BindComplete => {
            debug!(client = %self.client_addr, "BindComplete");
        }
        
        Message::CloseComplete => {
            debug!(client = %self.client_addr, "CloseComplete");
        }
        
        Message::AuthenticationOk => {
            debug!(client = %self.client_addr, "AuthenticationOk");
        }
        
        Message::AuthenticationCleartextPassword => {
            debug!(client = %self.client_addr, "AuthenticationCleartextPassword");
        }
        
        Message::AuthenticationMd5Password(body) => {
            debug!(
                client = %self.client_addr,
                salt = ?body.salt(),
                "AuthenticationMD5Password"
            );
        }
        
        Message::AuthenticationSasl(body) => {
            // ✅ FallibleIterator для механизмов
            let mut mechs = Vec::new();
            let mut mechs_iter = body.mechanisms();
            while let Ok(Some(mech)) = mechs_iter.next() {
                mechs.push(mech.to_string());
            }
            debug!(
                client = %self.client_addr,
                mechanisms = ?mechs,
                "AuthenticationSASL"
            );
        }
        
        Message::AuthenticationSaslContinue(body) => {
            debug!(
                client = %self.client_addr,
                data_len = body.data().len(),
                "AuthenticationSASLContinue"
            );
        }
        
        Message::AuthenticationSaslFinal(body) => {
            debug!(
                client = %self.client_addr,
                data_len = body.data().len(),
                "AuthenticationSASLFinal"
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
        
        Message::CopyData(_) => {
            debug!(client = %self.client_addr, "CopyData");
        }
        
        Message::CopyDone => {
            debug!(client = %self.client_addr, "CopyDone");
        }
        
        Message::CopyInResponse(body) => {
            debug!(
                client = %self.client_addr,
                format = body.format(),
                "CopyInResponse"
            );
        }
        
        Message::CopyOutResponse(body) => {
            debug!(
                client = %self.client_addr,
                format = body.format(),
                "CopyOutResponse"
            );
        }
        
        Message::EmptyQueryResponse => {
            debug!(client = %self.client_addr, "EmptyQueryResponse");
        }
        
        Message::NoData => {
            debug!(client = %self.client_addr, "NoData");
        }
        
        Message::NotificationResponse(body) => {
            if let (Ok(channel), Ok(msg)) = (body.channel(), body.message()) {
                info!(
                    client = %self.client_addr,
                    process_id = body.process_id(),
                    channel = %channel,
                    message = %msg,
                    "Notification"
                );
            }
        }
        
        Message::ParameterDescription(_) => {
            debug!(client = %self.client_addr, "ParameterDescription");
        }
        
        Message::PortalSuspended => {
            debug!(client = %self.client_addr, "PortalSuspended");
        }
        
        Message::AuthenticationKerberosV5 => {
            debug!(client = %self.client_addr, "AuthenticationKerberosV5");
        }
        
        Message::AuthenticationScmCredential => {
            debug!(client = %self.client_addr, "AuthenticationScmCredential");
        }
        
        Message::AuthenticationGss => {
            debug!(client = %self.client_addr, "AuthenticationGss");
        }
        
        Message::AuthenticationSspi => {
            debug!(client = %self.client_addr, "AuthenticationSspi");
        }
        
        Message::AuthenticationGssContinue(body) => {
            debug!(
                client = %self.client_addr,
                data_len = body.data().len(),
                "AuthenticationGssContinue"
            );
        }
        _ => {
            debug!(client = %self.client_addr, "Unknown server message");
        }
    }
}

}

async fn handle_client(
    client: TcpStream, 
    upstream_addr: std::net::SocketAddr,
    client_addr: std::net::SocketAddr,
    active_queries: ActiveQueries,
    session_id: u64,
    log_tx: mpsc::Sender<QueryLogEntry>,
) -> io::Result<()> {
    
    let server = TcpStream::connect(upstream_addr).await.map_err(|e| {
        error!(
            client = %client_addr,
            upstream = %upstream_addr,
            error = %e,
            "Failed to connect to upstream"
        );
        e
    })?;

    info!(client = %client_addr, upstream = %upstream_addr, "Connected to upstream");
    
    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();

    let (tx, rx) = mpsc::channel::<TrafficData>(100); 
    
    let parser_task = tokio::spawn(async move {
        traffic_parser(rx, active_queries, upstream_addr, session_id, log_tx).await;
    });

    let tx_c2s = tx.clone();
    let tx_s2c = tx;
    
    let client_to_server = tokio::spawn(async move {
        copy_with_logging(
            &mut client_read,
            &mut server_write,
            tx_c2s,
            TrafficDirection::ClientToServer,
            client_addr,
        ).await
    });

    let server_to_client = tokio::spawn(async move {
        copy_with_logging(
            &mut server_read,
            &mut client_write,
            tx_s2c,
            TrafficDirection::ServerToClient,
            client_addr,
        ).await
    });

    tokio::try_join!(client_to_server, server_to_client)?;
    
    info!(client = %client_addr, "Connection closed");
    drop(parser_task);

    Ok(())
}

async fn traffic_parser(
    mut rx: mpsc::Receiver<TrafficData>,
    active_queries: ActiveQueries,
    server_addr: std::net::SocketAddr,
    session_id: u64,
    log_tx: mpsc::Sender<QueryLogEntry>,
) {
    let mut parsers: HashMap<std::net::SocketAddr, PostgresParser> = HashMap::new();
    
    while let Some(traffic) = rx.recv().await {
        let parser = parsers
            .entry(traffic.client_addr)
            .or_insert_with(|| PostgresParser::new(
                traffic.client_addr,
                server_addr,
                active_queries.clone(),
                session_id,
                log_tx.clone(),
            ));
        
        match traffic.direction {
            TrafficDirection::ClientToServer => {
                parser.parse_client_message(&traffic.data);
            }
            TrafficDirection::ServerToClient => {
                parser.parse_server_message(&traffic.data);
            }
        }
    }
    
    // ✅ Удаляем save_to_csv - больше не нужно
    debug!("Traffic parser stopped");
}

// Аналог io::copy, но с отправкой данных в канал
async fn copy_with_logging<R, W>(
    reader: &mut R,
    writer: &mut W,
    tx: mpsc::Sender<TrafficData>,
    direction: TrafficDirection,
    client_addr: std::net::SocketAddr,
) -> io::Result<u64>
where
    R: AsyncReadExt + Unpin,  // Может читать асинхронно
    W: AsyncWriteExt + Unpin,  // Может писать асинхронно
{
    let mut buffer = vec![0u8; 8192];  // Буфер 8KB
    let mut total_bytes = 0u64;
    
    loop {
        // ШАГ 1: Читаем данные
        let n = reader.read(&mut buffer).await?;
        
        // ШАГ 2: Если 0 байт = EOF (конец потока)
        if n == 0 {
            break;
        }
        
        total_bytes += n as u64;
        
        // ШАГ 3: Пишем данные дальше (не блокируем передачу!)
        writer.write_all(&buffer[..n]).await?;
        
        // ШАГ 4: Отправляем копию данных в парсер (асинхронно)
        // Клонируем данные, чтобы не блокировать
        let traffic = TrafficData {
            direction: direction.clone(),
            client_addr,
            data: buffer[..n].to_vec(),  // Копируем только прочитанные байты
        };
        
        // Если канал переполнен или закрыт - игнорируем
        // (не хотим блокировать передачу данных из-за медленного парсера)
        let _ = tx.try_send(traffic);
    }
    
    Ok(total_bytes)
}


#[derive(Clone, Debug)]
struct QueryExecution {
    query_id: u64,
    query: String,
    user: String,
    database: String,
    application_name: String,
    client_addr: std::net::SocketAddr,
    server_addr: std::net::SocketAddr,
    start_time: Instant,
    backend_pid: Option<i32>,
    secret_key: Option<i32>, 
}

#[derive(Clone)]
struct ActiveQueries {
    queries: Arc<RwLock<HashMap<u64, QueryExecution>>>,
}

impl ActiveQueries {
    fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    async fn start_query(&self, exec: QueryExecution) {
        let mut queries = self.queries.write().await;
        queries.insert(exec.query_id, exec);
    }
    
    async fn finish_query(&self, query_id: u64) {
        let mut queries = self.queries.write().await;
        queries.remove(&query_id);
    }
    
    async fn get_query(&self, query_id: u64) -> Option<QueryExecution> {
        let queries = self.queries.read().await;
        queries.get(&query_id).cloned()
    }

    async fn get_top_running(&self, limit: usize) -> Vec<QueryExecution> {
        let queries = self.queries.read().await;
        let mut active: Vec<_> = queries.values().cloned().collect();
        
        active.sort_by(|a, b| {
            b.start_time.elapsed().cmp(&a.start_time.elapsed())
        });
        
        active.into_iter().take(limit).collect()
    }
}

#[derive(Clone, Serialize)]
struct ActiveQueryInfo {
    query_id: u64,
    query: String,
    user: String,
    database: String,
    application_name: String,
    client_addr: String,
    elapsed_seconds: f64,
}

use axum::{
    extract::{ws::WebSocket, ws::Message as WsMessage, WebSocketUpgrade, State},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};

async fn dashboard_html() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(active_queries): State<ActiveQueries>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket(socket, active_queries))
}

async fn handle_websocket(mut socket: WebSocket, active_queries: ActiveQueries) {
    info!("WebSocket client connected");
    
    // Отправляем обновления каждые 100ms
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
    
    loop {
        interval.tick().await;
        
        let top_queries = active_queries.get_top_running(10).await;
        
        let query_infos: Vec<ActiveQueryInfo> = top_queries
            .into_iter()
            .map(|q| ActiveQueryInfo {
                query_id: q.query_id,
                query: q.query,
                user: q.user,
                database: q.database,
                application_name: q.application_name,
                client_addr: q.client_addr.to_string(),
                elapsed_seconds: q.start_time.elapsed().as_secs_f64(),
            })
            .collect();
        
        let json = match serde_json::to_string(&query_infos) {
            Ok(j) => j,
            Err(e) => {
                error!(error = %e, "Failed to serialize query info");
                continue;
            }
        };
        
        if socket.send(WsMessage::Text(json)).await.is_err() {
            info!("WebSocket client disconnected");
            break;
        }
    }
}

async fn start_http_server(active_queries: ActiveQueries) {
    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/api/stats/live", get(websocket_handler))
        .route("/api/query/:query_id/cancel", post(cancel_query_endpoint))
        .route("/api/query/:query_id/blocking", get(get_blocking_endpoint))
        .route("/api/transaction/:backend_pid/history", get(get_transaction_history))  // ✅ Новый
        .with_state(active_queries);
    
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap_or_else(|e| fatal!(e, "Failed to bind HTTP server", addr = "127.0.0.1:8080"));
    
    info!("Dashboard available at http://127.0.0.1:8080");
    
    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| fatal!(e, "HTTP server failed"));
}


async fn send_cancel_request(
    upstream_addr: std::net::SocketAddr,
    backend_pid: i32,
    secret_key: i32,
) -> io::Result<()> {
    // Открываем новое TCP соединение к PostgreSQL
    let mut stream = TcpStream::connect(upstream_addr).await?;
    
    // Формируем CancelRequest packet
    let cancel_request = [
        16i32.to_be_bytes(),                    // Length = 16
        80877102i32.to_be_bytes(),              // Cancel request code
        backend_pid.to_be_bytes(),              // Backend PID
        secret_key.to_be_bytes(),               // Secret key
    ];
    
    // Отправляем
    for bytes in &cancel_request {
        stream.write_all(bytes).await?;
    }
    
    // Закрываем соединение (не ждем ответа)
    stream.shutdown().await?;
    
    info!(
        upstream = %upstream_addr,
        pid = backend_pid,
        "Cancel request sent"
    );
    
    Ok(())
}

#[derive(Serialize)]
struct CancelResponse {
    success: bool,
    message: String,
}

async fn cancel_query_endpoint(
    Path(query_id): Path<u64>,
    State(active_queries): State<ActiveQueries>,
) -> Result<Json<CancelResponse>, StatusCode> {
    // Получаем информацию о запросе
    let query_exec = match active_queries.get_query(query_id).await {
        Some(q) => q,
        None => {
            return Ok(Json(CancelResponse {
                success: false,
                message: format!("Query {} not found or already completed", query_id),
            }));
        }
    };
    
    // Проверяем что есть PID и Secret
    let backend_pid = match query_exec.backend_pid {
        Some(pid) => pid,
        None => {
            return Ok(Json(CancelResponse {
                success: false,
                message: "Backend PID not available".to_string(),
            }));
        }
    };
    
    let secret_key = match query_exec.secret_key {
        Some(key) => key,
        None => {
            return Ok(Json(CancelResponse {
                success: false,
                message: "Secret key not available".to_string(),
            }));
        }
    };
    
    // Отправляем CancelRequest
    match send_cancel_request(query_exec.server_addr, backend_pid, secret_key).await {
        Ok(_) => {
            info!(
                query_id = query_id,
                backend_pid = backend_pid,
                "Query cancelled"
            );
            
            Ok(Json(CancelResponse {
                success: true,
                message: format!("Cancel request sent for query {}", query_id),
            }))
        }
        Err(e) => {
            error!(
                query_id = query_id,
                error = %e,
                "Failed to send cancel request"
            );
            
            Ok(Json(CancelResponse {
                success: false,
                message: format!("Failed to cancel: {}", e),
            }))
        }
    }
}

#[derive(Clone, Serialize, Debug)]
struct BlockedQuery {
    blocked_pid: i32,
    blocked_query: String,
    blocked_user: String,
    blocked_duration_seconds: f64,
    wait_event: Option<String>,
    lock_type: String,
}

#[derive(Clone, Serialize, Debug)]
struct BlockingInfo {
    blocking_pid: i32,
    blocking_query_id: Option<u64>,  // Наш внутренний ID
    blocked_queries: Vec<BlockedQuery>,
}


async fn get_blocking_info(
    upstream_addr: std::net::SocketAddr,
    backend_pid: i32,
) -> Result<Vec<BlockedQuery>, Box<dyn std::error::Error>> {
    // Формируем connection string
    let config = format!(
        "host={} port={} user=dev dbname=postgres",
        upstream_addr.ip(),
        upstream_addr.port()
    );
    
    info!(
        upstream = %upstream_addr,
        backend_pid = backend_pid,
        "Connecting to PostgreSQL to get blocking info"
    );
    
    // Подключаемся к PostgreSQL
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;
    
    // Запускаем connection в отдельной задаче
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(error = %e, "PostgreSQL connection error");
        }
    });

    // SQL запрос для получения заблокированных процессов
    let query = r#"
        WITH blocking_pids AS (
            SELECT DISTINCT
                blocking.pid AS blocking_pid,
                blocked.pid AS blocked_pid
            FROM pg_locks blocked
            JOIN pg_locks blocking 
                ON blocking.locktype = blocked.locktype
                AND blocking.database IS NOT DISTINCT FROM blocked.database
                AND blocking.relation IS NOT DISTINCT FROM blocked.relation
                AND blocking.page IS NOT DISTINCT FROM blocked.page
                AND blocking.tuple IS NOT DISTINCT FROM blocked.tuple
                AND blocking.virtualxid IS NOT DISTINCT FROM blocked.virtualxid
                AND blocking.transactionid IS NOT DISTINCT FROM blocked.transactionid
                AND blocking.classid IS NOT DISTINCT FROM blocked.classid
                AND blocking.objid IS NOT DISTINCT FROM blocked.objid
                AND blocking.objsubid IS NOT DISTINCT FROM blocked.objsubid
                AND blocking.pid != blocked.pid
            WHERE NOT blocked.granted
              AND blocking.granted
              AND blocked.pid = $1
        )
        SELECT 
            bp.blocked_pid,
            sa.query AS blocked_query,
            sa.usename AS blocked_user,
            EXTRACT(EPOCH FROM (now() - sa.query_start))::float8 AS blocked_duration_seconds,
            sa.wait_event,
            COALESCE(l.locktype, 'unknown') AS lock_type
        FROM blocking_pids bp
        JOIN pg_stat_activity sa ON sa.pid = bp.blocked_pid
        LEFT JOIN pg_locks l ON l.pid = bp.blocked_pid AND NOT l.granted
        WHERE sa.state != 'idle'
        ORDER BY blocked_duration_seconds DESC;
    "#;
    
    info!(
        backend_pid = backend_pid,
        "Executing blocking query"
    );
    
    let rows = client.query(query, &[&backend_pid]).await
    .map_err(|e| {
        error!("query error: {:?}", e);
        e
    })?;
    
    info!(
        backend_pid = backend_pid,
        rows_count = rows.len(),
        "Retrieved blocking info"
    );
    
    let mut blocked_queries = Vec::new();
    
    for row in rows {
        let blocked_query = BlockedQuery {
            blocked_pid: row.get("blocked_pid"),
            blocked_query: row.get("blocked_query"),
            blocked_user: row.get("blocked_user"),
            blocked_duration_seconds: row.get::<_, f64>("blocked_duration_seconds"),
            wait_event: row.get("wait_event"),
            lock_type: row.get("lock_type"),
        };
        
        debug!(
            blocked_pid = blocked_query.blocked_pid,
            blocked_user = %blocked_query.blocked_user,
            duration = blocked_query.blocked_duration_seconds,
            "Found blocked query"
        );
        
        blocked_queries.push(blocked_query);
    }
    
    Ok(blocked_queries)
}

async fn get_detailed_blocking_info(
    upstream_addr: std::net::SocketAddr,
    backend_pid: i32,
) -> Result<DetailedBlockingInfo, Box<dyn std::error::Error>> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&upstream_addr.ip().to_string())
        .port(upstream_addr.port())
        .user("dev")
        .password("dev")
        .dbname("postgres")
        .connect_timeout(std::time::Duration::from_secs(5));
    
    info!(
        host = %upstream_addr.ip(),
        port = upstream_addr.port(),
        backend_pid = backend_pid,
        "Connecting to PostgreSQL for detailed blocking info"
    );
    
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(error = %e, "PostgreSQL connection error");
        }
    });
    
    // ✅ Получаем информацию о ТЕКУЩЕМ (заблокированном) процессе
    let current_process_query = r#"
        SELECT 
            pid,
            usename,
            COALESCE(query, '<idle>') as query,
            state,
            EXTRACT(EPOCH FROM (now() - COALESCE(query_start, xact_start)))::float8 as duration,
            client_addr::text,
            application_name,
            wait_event,
            wait_event_type
        FROM pg_stat_activity
        WHERE pid = $1
    "#;
    
    let current_row = client.query_one(current_process_query, &[&backend_pid]).await?;
    
    let current_user: String = current_row.get("usename");
    let current_query: String = current_row.get("query");
    let current_state: String = current_row.get("state");
    let current_duration: f64 = current_row.get("duration");
    let wait_event: Option<String> = current_row.get("wait_event");
    
 let blockers_query = r#"
    WITH 
    -- Locks которые ждет текущий процесс
    waiting_locks AS (
        SELECT 
            l.pid,
            l.locktype,
            l.mode,
            l.relation,
            l.transactionid,
            l.virtualxid,
            l.page,
            l.tuple,
            COALESCE(c.relname, '') as relation_name,
            COALESCE(c.relkind::text, '') as relation_kind  -- ✅ ::text каст
        FROM pg_locks l
        LEFT JOIN pg_class c ON c.oid = l.relation
        WHERE l.pid = $1 AND l.granted = false
    ),
    -- Процессы которые держат конфликтующие locks
    blocking_processes AS (
        SELECT DISTINCT
            blocking.pid AS blocking_pid,
            wl.locktype,
            wl.mode as waiting_mode,
            blocking.mode as held_mode,
            wl.relation,
            wl.relation_name,
            wl.relation_kind
        FROM waiting_locks wl
        JOIN pg_locks blocking ON (
            blocking.granted = true
            AND blocking.pid != $1
            AND blocking.locktype = wl.locktype
            AND (
                (blocking.relation IS NOT DISTINCT FROM wl.relation) OR
                (blocking.transactionid IS NOT DISTINCT FROM wl.transactionid) OR
                (blocking.virtualxid IS NOT DISTINCT FROM wl.virtualxid) OR
                (blocking.page IS NOT DISTINCT FROM wl.page AND blocking.tuple IS NOT DISTINCT FROM wl.tuple)
            )
        )
    )
    SELECT 
        -- Информация о блокирующем процессе
        bp.blocking_pid,
        sa.usename AS blocking_user,
        COALESCE(sa.query, '<idle>') AS blocking_query,
        sa.state AS blocking_state,
        EXTRACT(EPOCH FROM (now() - COALESCE(sa.query_start, sa.xact_start)))::float8 AS blocking_duration,
        COALESCE(sa.client_addr::text, '') AS blocking_client_addr,  -- ✅ может быть NULL
        COALESCE(sa.application_name, '') AS blocking_application,   -- ✅ может быть NULL
        
        -- Lock который держит блокирующий процесс
        bp.locktype AS blocking_locktype,
        bp.held_mode AS blocking_mode,
        bp.relation AS blocking_relation,
        bp.relation_name AS blocking_relation_name,
        bp.relation_kind AS blocking_relation_kind,  -- ✅ уже обработано в CTE
        
        -- Lock который ждем мы
        bp.waiting_mode,
        bp.relation_name AS waiting_relation_name,
        bp.relation_kind AS waiting_relation_kind    -- ✅ уже обработано в CTE
    FROM blocking_processes bp
    JOIN pg_stat_activity sa ON sa.pid = bp.blocking_pid
    ORDER BY blocking_duration DESC
"#;
    
    let blocker_rows = client.query(blockers_query, &[&backend_pid]).await?;
    
    if blocker_rows.is_empty() {
        // ✅ Никто не блокирует
        return Ok(DetailedBlockingInfo {
            blocking_pid: backend_pid,
            blocking_query_id: None,
            current_process: CurrentProcessInfo {
                pid: backend_pid,
                user: current_user,
                query: current_query,
                state: current_state,
                duration: current_duration,
                wait_event,
            },
            blockers: Vec::new(),
            waiting_locks: Vec::new(),
        });
    }
    use std::collections::HashSet;

    // ✅ Собираем информацию о блокирующих процессах
    let mut blockers_map: HashMap<i32, BlockerDetail> = HashMap::new();
    let mut waiting_locks_set: HashSet<String> = HashSet::new();
    let mut waiting_locks: Vec<LockDetail> = Vec::new();
    
    for row in blocker_rows {
        let blocking_pid: i32 = row.get("blocking_pid");
        
        // Добавляем waiting lock (только уникальные)
        let waiting_mode: String = row.get("waiting_mode");
        let locktype: String = row.get("blocking_locktype");
        let relation_name: String = row.get("waiting_relation_name");
        let relation_kind: String = row.get("blocking_relation_kind");
        
        let lock_key = format!("{}-{}-{}", locktype, waiting_mode, relation_name);
        if !waiting_locks_set.contains(&lock_key) {
            waiting_locks_set.insert(lock_key);
            
            let object_type = match relation_kind.as_str() {
                "r" => "table",
                "i" => "index",
                "S" => "sequence",
                "v" => "view",
                "m" => "materialized view",
                _ => "object",
            };
            
            waiting_locks.push(LockDetail {
                lock_type: locktype.clone(),
                lock_mode: waiting_mode,
                granted: false,
                object_type: object_type.to_string(),
                object_name: if relation_name.is_empty() {
                    None
                } else {
                    Some(relation_name.clone())
                },
                relation_oid:  row.try_get("blocking_relation").ok().flatten(),
            });
        }
        
        // Добавляем/обновляем blocker
        let blocker = blockers_map.entry(blocking_pid).or_insert_with(|| {
            BlockerDetail {
                blocking_pid,
                blocking_user: row.get("blocking_user"),
                blocking_query: row.get("blocking_query"),
                blocking_state: row.get("blocking_state"),
                blocking_duration: row.get("blocking_duration"),
                blocking_client_addr: row.get("blocking_client_addr"),
                blocking_application: row.get("blocking_application"),
                held_locks: Vec::new(),
            }
        });
        
        // Добавляем held lock
        let blocking_mode: String = row.get("blocking_mode");
        let blocking_relation_name: String = row.get("blocking_relation_name");
        let blocking_relation_kind: String = row.get("blocking_relation_kind");
        
        let blocker_object_type = match blocking_relation_kind.as_str() {
            "r" => "table",
            "i" => "index",
            "S" => "sequence",
            "v" => "view",
            "m" => "materialized view",
            _ => "object",
        };
        
        blocker.held_locks.push(LockDetail {
            lock_type: locktype.clone(),
            lock_mode: blocking_mode,
            granted: true,
            object_type: blocker_object_type.to_string(),
            object_name: if blocking_relation_name.is_empty() {
                None
            } else {
                Some(blocking_relation_name)
            },
            relation_oid:  row.try_get("blocking_relation").ok().flatten(),
        });
    }
    
    let blockers: Vec<BlockerDetail> = blockers_map.into_values().collect();
    
    info!(
        backend_pid = backend_pid,
        blockers_count = blockers.len(),
        "Found blocking processes"
    );
    
    Ok(DetailedBlockingInfo {
        blocking_pid: backend_pid,
        blocking_query_id: None,
        current_process: CurrentProcessInfo {
            pid: backend_pid,
            user: current_user,
            query: current_query,
            state: current_state,
            duration: current_duration,
            wait_event,
        },
        blockers,
        waiting_locks,
    })
}

async fn get_blocking_endpoint(
    Path(query_id): Path<u64>,
    State(active_queries): State<ActiveQueries>,
) -> Result<Json<DetailedBlockingInfo>, StatusCode> {
    let query_exec = match active_queries.get_query(query_id).await {
        Some(q) => q,
        None => {
            warn!(query_id = query_id, "Query not found");
            return Err(StatusCode::NOT_FOUND);
        }
    };
    
    let backend_pid = match query_exec.backend_pid {
        Some(pid) => pid,
        None => {
            warn!(query_id = query_id, "Backend PID not available");
            return Err(StatusCode::NOT_FOUND);
        }
    };
    
    let blocking_info = match get_detailed_blocking_info(query_exec.server_addr, backend_pid).await {
        Ok(info) => info,
        Err(e) => {
            error!(
                query_id = query_id,
                backend_pid = backend_pid,
                error = %e,
                "Failed to get detailed blocking info"
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    
    Ok(Json(blocking_info))
}

#[derive(Clone, Serialize, Debug)]
struct LockDetail {
    lock_type: String,           // relation, tuple, transactionid, etc
    lock_mode: String,           // AccessShareLock, RowExclusiveLock, etc
    granted: bool,               // Получен или ждет
    object_type: String,         // table, index, tuple
    object_name: Option<String>, // Имя таблицы/индекса
    relation_oid: Option<i32>,
}

#[derive(Clone, Serialize, Debug)]
struct BlockerDetail {
    blocking_pid: i32,
    blocking_user: String,
    blocking_query: String,
    blocking_state: String,      // active, idle in transaction
    blocking_duration: f64,      // Сколько держит блокировку
    blocking_client_addr: Option<String>,
    blocking_application: Option<String>,
    
    // Locks которые он держит
    held_locks: Vec<LockDetail>,
}

#[derive(Clone, Serialize, Debug)]
struct BlockedQueryDetail {
    blocked_pid: i32,
    blocked_query: String,
    blocked_user: String,
    blocked_duration_seconds: f64,
    wait_event: Option<String>,
    blocked_client_addr: Option<String>,
    blocked_application: Option<String>,
    
    // Locks которые он ждет
    waiting_locks: Vec<LockDetail>,
    
    // Кто его блокирует (может быть несколько процессов!)
    blockers: Vec<BlockerDetail>,
}

#[derive(Clone, Serialize, Debug)]
struct CurrentProcessInfo {
    pid: i32,
    user: String,
    query: String,
    state: String,
    duration: f64,
    wait_event: Option<String>,
}

#[derive(Clone, Serialize, Debug)]
struct DetailedBlockingInfo {
    blocking_pid: i32,  // PID текущего процесса (который заблокирован)
    blocking_query_id: Option<u64>,
    
    // ✅ Информация о текущем (заблокированном) процессе
    current_process: CurrentProcessInfo,
    
    // ✅ Кто его блокирует
    blockers: Vec<BlockerDetail>,
    
    // ✅ Какие locks он ждет
    waiting_locks: Vec<LockDetail>,
}

struct LogWriter {
    db: AsyncConnection,
    rx: mpsc::Receiver<QueryLogEntry>,
    batch_size: usize,
    flush_interval: tokio::time::Duration,
}

impl LogWriter {
    async fn new(
        db_path: &str,
        rx: mpsc::Receiver<QueryLogEntry>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Открываем БД в async режиме
        let db = AsyncConnection::open(db_path).await?;
        
        // Инициализируем схему
        db.call(|conn| {
            conn.execute_batch(DB_SCHEMA)?;
            
            // ✅ Включаем WAL mode для лучшей производительности
            conn.execute_batch(r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA cache_size = 10000;
                PRAGMA temp_store = MEMORY;
            "#)?;
            
            Ok(())
        }).await?;
        
        info!("SQLite database initialized at {}", db_path);
        
        Ok(Self {
            db,
            rx,
            batch_size: 100,  // Пишем батчами по 100 записей
            flush_interval: tokio::time::Duration::from_millis(100),  // Или каждые 100ms
        })
    }
    
    async fn run(mut self) {
        let mut buffer = Vec::with_capacity(self.batch_size);
        let mut flush_timer = tokio::time::interval(self.flush_interval);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            tokio::select! {
                // Получаем записи из канала
                Some(entry) = self.rx.recv() => {
                    buffer.push(entry);
                    
                    // Если буфер заполнен - пишем немедленно
                    if buffer.len() >= self.batch_size {
                        if let Err(e) = self.flush_batch(&mut buffer).await {
                            error!(error = %e, "Failed to flush batch");
                        }
                    }
                }
                
                // Или по таймеру
                _ = flush_timer.tick() => {
                    if !buffer.is_empty() {
                        if let Err(e) = self.flush_batch(&mut buffer).await {
                            error!(error = %e, "Failed to flush batch on timer");
                        }
                    }
                }
                
                // Канал закрылся - записываем остатки и выходим
                else => {
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
        let entries = std::mem::take(buffer);  // Забираем все записи
        let count = entries.len();
        
        let start = Instant::now();
        
        // ✅ Пишем батчом в одной транзакции
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
            Ok(())
        }).await?;
        
        let elapsed = start.elapsed();
        debug!(
            count = count,
            elapsed_ms = elapsed.as_millis(),
            "Flushed batch to SQLite"
        );
        
        Ok(())
    }
}

#[derive(Serialize)]
struct TransactionHistoryResponse {
    backend_pid: i32,
    query_count: usize,
    query_logs: Vec<QueryLogDto>,
}

#[derive(Serialize)]
struct QueryLogDto {
    query_id: String,
    query_text: String,
    query_type: String,
    execution_time_ms: f64,
    status: String,
    error_message: Option<String>,
    timestamp: String,
}

async fn get_transaction_history(
    Path(backend_pid): Path<i32>,
) -> Result<Json<TransactionHistoryResponse>, StatusCode> {
    // ✅ Читаем из SQLite
    let db = AsyncConnection::open("proxy_logs.db")
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to open database");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    let logs = db.call(move |conn| {
        let mut stmt = conn.prepare(r#"
            SELECT 
                query_id, query_text, query_type,
                execution_time_us, status, error_message,
                start_time
            FROM query_logs
            WHERE backend_pid = ?
            ORDER BY start_time ASC
        "#)?;
        
        let rows = stmt.query_map([backend_pid], |row| {
            Ok(QueryLogDto {
                query_id: row.get(0)?,
                query_text: row.get(1)?,
                query_type: row.get(2)?,
                execution_time_ms: row.get::<_, i64>(3)? as f64 / 1000.0,
                status: row.get(4)?,
                error_message: row.get(5)?,
                timestamp: {
                    let us: i64 = row.get(6)?;
                    let secs = us / 1_000_000;
                    let dt = chrono::NaiveDateTime::from_timestamp_opt(secs, 0);
                    dt.map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                },
            })
        })?;
        
        let mut logs = Vec::new();
        for row in rows {
            logs.push(row?);
        }
        
        Ok((logs))
    }).await.map_err(|e| {
        error!(error = %e, backend_pid = backend_pid, "Failed to query transaction history");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    if logs.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    
    Ok(Json(TransactionHistoryResponse {
        backend_pid,
        query_count: logs.len(),
        query_logs: logs,
    }))
}