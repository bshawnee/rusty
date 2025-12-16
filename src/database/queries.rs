use serde::Serialize;
use tokio_postgres::{NoTls, Error as PgError};
use tokio_rusqlite::Connection as AsyncConnection;
use tracing::{debug, error, info};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Serialize, Debug)]
pub struct LockDetail {
    pub lock_type: String,
    pub lock_mode: String,
    pub granted: bool,
    pub object_type: String,
    pub object_name: Option<String>,
    pub relation_oid: Option<i32>,
}

#[derive(Clone, Serialize, Debug)]
pub struct BlockerDetail {
    pub blocking_pid: i32,
    pub blocking_user: String,
    pub blocking_query: String,
    pub blocking_state: String,
    pub blocking_duration: f64,
    pub blocking_client_addr: Option<String>,
    pub blocking_application: Option<String>,
    pub held_locks: Vec<LockDetail>,
}

#[derive(Clone, Serialize, Debug)]
pub struct CurrentProcessInfo {
    pub pid: i32,
    pub user: String,
    pub query: String,
    pub state: String,
    pub duration: f64,
    pub wait_event: Option<String>,
}

#[derive(Clone, Serialize, Debug)]
pub struct DetailedBlockingInfo {
    pub blocking_pid: i32,
    pub blocking_query_id: Option<u64>,
    pub current_process: CurrentProcessInfo,
    pub blockers: Vec<BlockerDetail>,
    pub waiting_locks: Vec<LockDetail>,
}

#[derive(Serialize, Clone)]
pub struct BlockerTransactionInfo {
    pub backend_pid: i32,
    pub blocking_user: String,
    pub blocking_state: String,
    pub transaction_duration: f64,
    pub total_queries: usize,
    pub recent_queries: Vec<QueryLogDto>,
    pub current_query: String,
    pub client_addr: Option<String>,
    pub application_name: Option<String>,
}


pub async fn get_detailed_blocking_info(
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
    
    let (client, connection) = config.connect(NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(error = %e, "PostgreSQL connection error");
        }
    });
    
    // Получаем информацию о текущем процессе
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
            COALESCE(c.relkind::text, '') as relation_kind
        FROM pg_locks l
        LEFT JOIN pg_class c ON c.oid = l.relation
        WHERE l.pid = $1 AND l.granted = false
    ),
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
        bp.blocking_pid,
        sa.usename AS blocking_user,
        COALESCE(sa.query, '<idle>') AS blocking_query,
        sa.state AS blocking_state,
        EXTRACT(EPOCH FROM (now() - COALESCE(sa.query_start, sa.xact_start)))::float8 AS blocking_duration,
        COALESCE(
            CONCAT(
                split_part(sa.client_addr::text, '/', 1),
                ':',
                sa.client_port::text
            ), 
            ''
        ) AS blocking_client_addr,
        COALESCE(sa.application_name, '') AS blocking_application,
        bp.locktype AS blocking_locktype,
        bp.held_mode AS blocking_mode,
        bp.relation AS blocking_relation,
        bp.relation_name AS blocking_relation_name,
        bp.relation_kind AS blocking_relation_kind,
        bp.waiting_mode,
        bp.relation_name AS waiting_relation_name,
        bp.relation_kind AS waiting_relation_kind
    FROM blocking_processes bp
    JOIN pg_stat_activity sa ON sa.pid = bp.blocking_pid
    ORDER BY blocking_duration DESC
    "#;
    
    let blocker_rows = client.query(blockers_query, &[&backend_pid]).await?;
    
    if blocker_rows.is_empty() {
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
    
    let mut blockers_map: HashMap<i32, BlockerDetail> = HashMap::new();
    let mut waiting_locks_set: HashSet<String> = HashSet::new();
    let mut waiting_locks: Vec<LockDetail> = Vec::new();
    
    for row in blocker_rows {
        let blocking_pid: i32 = row.get("blocking_pid");
        
        // Обработка waiting locks
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
                relation_oid: row.try_get("blocking_relation").ok().flatten(),
            });
        }
        
        // Обработка blockers
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
            relation_oid: row.try_get("blocking_relation").ok().flatten(),
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

#[derive(Serialize)]
pub struct TransactionHistoryResponse {
    pub backend_pid: i32,
    pub query_count: usize,
    pub query_logs: Vec<QueryLogDto>,
}

#[derive(Serialize, Clone)]
pub struct QueryLogDto {
    pub query_id: String,
    pub query_text: String,
    pub query_type: String,
    pub execution_time_ms: f64,
    pub status: String,
    pub error_message: Option<String>,
    pub timestamp: String,
}

pub async fn get_transaction_history_from_db(
    backend_pid: i32,
) -> Result<TransactionHistoryResponse, Box<dyn std::error::Error>> {
    let db = AsyncConnection::open("proxy_logs.db").await?;
    
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
        
        Ok(logs)
    }).await?;
    
    if logs.is_empty() {
        return Err("No logs found for this backend_pid".into());
    }
    
    Ok(TransactionHistoryResponse {
        backend_pid,
        query_count: logs.len(),
        query_logs: logs,
    })
}

// Добавьте в конец файла src/database/queries.rs


/// Получает детальную информацию о транзакции блокера
pub async fn get_blocker_transaction_details(
    upstream_addr: std::net::SocketAddr,
    backend_pid: i32,
    limit: usize,
) -> Result<BlockerTransactionInfo, Box<dyn std::error::Error>> {
    // 1. Получаем текущее состояние процесса из PostgreSQL
    let mut config = tokio_postgres::Config::new();
    config
        .host(&upstream_addr.ip().to_string())
        .port(upstream_addr.port())
        .user("dev")
        .password("dev")
        .dbname("postgres")
        .connect_timeout(std::time::Duration::from_secs(5));
    
    let (client, connection) = config.connect(NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(error = %e, "PostgreSQL connection error");
        }
    });
    
    // Получаем информацию о текущей транзакции
    let current_info_query = r#"
        SELECT 
            pid,
            usename,
            state,
            COALESCE(query, '<idle>') as current_query,
            EXTRACT(EPOCH FROM (now() - COALESCE(xact_start, query_start)))::float8 as transaction_duration,
            COALESCE(
                CONCAT(
                    split_part(client_addr::text, '/', 1),
                    ':',
                    client_port::text
                ), 
                ''
            ) as client_addr,
            COALESCE(application_name, '') as application_name
        FROM pg_stat_activity
        WHERE pid = $1
    "#;
    
    let row = client.query_one(current_info_query, &[&backend_pid]).await?;
    
    let blocking_user: String = row.get("usename");
    let blocking_state: String = row.get("state");
    let current_query: String = row.get("current_query");
    let transaction_duration: f64 = row.get("transaction_duration");
    let client_addr: String = row.get("client_addr");
    let application_name: String = row.get("application_name");
    
    // 2. Получаем историю запросов из SQLite
    let db = AsyncConnection::open("proxy_logs.db").await?;
    
    let pid = backend_pid;
    let query_limit = limit;
    
    let (total_queries, recent_queries) = db.call(move |conn| {
        // Считаем общее количество запросов в этой транзакции
        let total: usize = conn.query_row(
            "SELECT COUNT(*) FROM query_logs WHERE backend_pid = ?",
            [pid],
            |row| row.get(0)
        )?;
        
        // Получаем последние N запросов
        let mut stmt = conn.prepare(r#"
            SELECT 
                query_id, query_text, query_type,
                execution_time_us, status, error_message,
                start_time
            FROM query_logs
            WHERE backend_pid = ?
            ORDER BY start_time DESC
            LIMIT ?
        "#)?;
        
        let rows = stmt.query_map(rusqlite::params![pid, query_limit], |row| {
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
        
        let mut queries = Vec::new();
        for row in rows {
            queries.push(row?);
        }
        
        // Переворачиваем чтобы показать в хронологическом порядке
        queries.reverse();
        
        Ok((total, queries))
    }).await?;
    
    info!(
        backend_pid = backend_pid,
        total_queries = total_queries,
        recent_queries = recent_queries.len(),
        "Retrieved blocker transaction details"
    );
    
    Ok(BlockerTransactionInfo {
        backend_pid,
        blocking_user,
        blocking_state,
        transaction_duration,
        total_queries,
        recent_queries,
        current_query,
        client_addr: if client_addr.is_empty() { None } else { Some(client_addr) },
        application_name: if application_name.is_empty() { None } else { Some(application_name) },
    })
}