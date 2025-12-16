use axum::{
    extract::{State, Path, Query},
    response::Json,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct HistoryQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,  // "success", "error"
    user: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct QueryHistoryItem {
    pub query_id: String,
    pub query_text: String,
    pub query_type: String,
    pub user_name: String,
    pub database_name: String,
    pub application_name: String,
    pub client_addr: String,
    pub execution_time_ms: f64,
    pub status: String,
    pub error_message: Option<String>,
    pub timestamp: String,
    pub backend_pid: Option<i32>,
}

pub async fn get_query_history(
    State(state): State<AppState>,
    Query(params): Query<HistoryQuery>,
) -> Result<Json<Vec<QueryHistoryItem>>, StatusCode> {
    let limit = params.limit.unwrap_or(50).min(500);
    let offset = params.offset.unwrap_or(0);
    
    let sqlite = state.sqlite.clone();
    
    let items = tokio::task::spawn_blocking(move || {
        sqlite.call(move |conn| {
            let mut sql = String::from(r#"
                SELECT 
                    query_id,
                    query_text,
                    query_type,
                    user_name,
                    database_name,
                    application_name,
                    client_addr,
                    execution_time_us,
                    status,
                    error_message,
                    start_time,
                    backend_pid
                FROM query_logs
                WHERE 1=1
            "#);
            
            let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            
            // Add filters
            if let Some(ref status) = params.status {
                sql.push_str(" AND status = ?");
                params_vec.push(Box::new(status.clone()));
            }
            
            if let Some(ref user) = params.user {
                sql.push_str(" AND user_name = ?");
                params_vec.push(Box::new(user.clone()));
            }
            
            sql.push_str(" ORDER BY start_time DESC LIMIT ? OFFSET ?");
            params_vec.push(Box::new(limit as i64));
            params_vec.push(Box::new(offset as i64));
            
            let mut stmt = conn.prepare(&sql)?;
            
            let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec
                .iter()
                .map(|p| p.as_ref())
                .collect();
            
            let rows = stmt.query_map(params_refs.as_slice(), |row| {
                let query_id: String = row.get(0)?;
                let query_text: String = row.get(1)?;
                let query_type: String = row.get(2)?;
                let user_name: String = row.get(3)?;
                let database_name: String = row.get(4)?;
                let application_name: String = row.get(5)?;
                let client_addr: String = row.get(6)?;
                let execution_time_us: i64 = row.get(7)?;
                let status: String = row.get(8)?;
                let error_message: Option<String> = row.get(9)?;
                let start_time: i64 = row.get(10)?;
                let backend_pid: Option<i32> = row.get(11)?;
                
                Ok(QueryHistoryItem {
                    query_id,
                    query_text,
                    query_type,
                    user_name,
                    database_name,
                    application_name,
                    client_addr,
                    execution_time_ms: execution_time_us as f64 / 1000.0,
                    status,
                    error_message,
                    timestamp: {
                        let secs = start_time / 1_000_000;
                        let dt = chrono::NaiveDateTime::from_timestamp_opt(secs, 0);
                        dt.map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    },
                    backend_pid,
                })
            })?;
            
            let mut items = Vec::new();
            for row in rows {
                items.push(row?);
            }
            
            Ok::<_, rusqlite::Error>(items)
        })
    })
    .await
    .map_err(|e| {
        error!(error = %e, "Failed to spawn blocking task");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .map_err(|e| {
        error!(error = %e, "Failed to get query history");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    Ok(Json(items))
}

pub async fn get_query_detail(
    State(state): State<AppState>,
    Path(query_id): Path<String>,
) -> Result<Json<QueryHistoryItem>, StatusCode> {
    let sqlite = state.sqlite.clone();
    
    let item = tokio::task::spawn_blocking(move || {
        sqlite.call(move |conn| {
            let mut stmt = conn.prepare(r#"
                SELECT 
                    query_id,
                    query_text,
                    query_type,
                    user_name,
                    database_name,
                    application_name,
                    client_addr,
                    execution_time_us,
                    status,
                    error_message,
                    start_time,
                    backend_pid
                FROM query_logs
                WHERE query_id = ?
            "#)?;
            
            let result = stmt.query_row([&query_id], |row| {
                let query_id: String = row.get(0)?;
                let query_text: String = row.get(1)?;
                let query_type: String = row.get(2)?;
                let user_name: String = row.get(3)?;
                let database_name: String = row.get(4)?;
                let application_name: String = row.get(5)?;
                let client_addr: String = row.get(6)?;
                let execution_time_us: i64 = row.get(7)?;
                let status: String = row.get(8)?;
                let error_message: Option<String> = row.get(9)?;
                let start_time: i64 = row.get(10)?;
                let backend_pid: Option<i32> = row.get(11)?;
                
                Ok(QueryHistoryItem {
                    query_id,
                    query_text,
                    query_type,
                    user_name,
                    database_name,
                    application_name,
                    client_addr,
                    execution_time_ms: execution_time_us as f64 / 1000.0,
                    status,
                    error_message,
                    timestamp: {
                        let secs = start_time / 1_000_000;
                        let dt = chrono::NaiveDateTime::from_timestamp_opt(secs, 0);
                        dt.map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    },
                    backend_pid,
                })
            });
            
            match result {
                Ok(item) => Ok(Some(item)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e),
            }
        })
    })
    .await
    .map_err(|e| {
        error!(error = %e, "Failed to spawn blocking task");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .map_err(|e| {
        error!(error = %e, "Failed to get query detail");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    match item {
        Some(i) => Ok(Json(i)),
        None => Err(StatusCode::NOT_FOUND),
    }
}