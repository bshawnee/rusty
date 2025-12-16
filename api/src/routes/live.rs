use axum::{
    extract::{State, Query, WebSocketUpgrade},
    response::{IntoResponse, Json},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::state::AppState;
use pg_rusty_proxy_shared::QuerySnapshot;

#[derive(Deserialize)]
pub struct EventsQuery {
    since_us: Option<u64>,
}

#[derive(Serialize)]
pub struct ActiveQueriesResponse {
    queries: Vec<QuerySnapshot>,
    count: usize,
}

pub async fn get_active_queries(
    State(state): State<AppState>,
) -> Result<Json<ActiveQueriesResponse>, StatusCode> {
    let rocksdb = state.rocksdb.clone();
    
    // Run in blocking task
    let queries = tokio::task::spawn_blocking(move || {
        rocksdb.get_active_queries()
    })
    .await
    .map_err(|e| {
        error!(error = %e, "Failed to spawn blocking task");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .map_err(|e| {
        error!(error = %e, "Failed to get active queries");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    let count = queries.len();
    
    Ok(Json(ActiveQueriesResponse {
        queries,
        count,
    }))
}

pub async fn get_events(
    State(state): State<AppState>,
    Query(params): Query<EventsQuery>,
) -> Result<Json<Vec<pg_rusty_proxy_shared::Event>>, StatusCode> {
    let since_us = params.since_us.unwrap_or(0);
    let rocksdb = state.rocksdb.clone();
    
    let events = tokio::task::spawn_blocking(move || {
        rocksdb.get_events_since(since_us)
    })
    .await
    .map_err(|e| {
        error!(error = %e, "Failed to spawn blocking task");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .map_err(|e| {
        error!(error = %e, "Failed to get events");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    Ok(Json(events))
}

pub async fn get_metrics(
    State(state): State<AppState>,
) -> Result<Json<std::collections::HashMap<String, u64>>, StatusCode> {
    let rocksdb = state.rocksdb.clone();
    
    let metrics = tokio::task::spawn_blocking(move || {
        rocksdb.get_all_metrics()
    })
    .await
    .map_err(|e| {
        error!(error = %e, "Failed to spawn blocking task");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .map_err(|e| {
        error!(error = %e, "Failed to get metrics");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    Ok(Json(metrics))
}

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

async fn handle_websocket(
    mut socket: axum::extract::ws::WebSocket,
    state: AppState,
) {
    use axum::extract::ws::Message;
    use tokio::time::{interval, Duration};
    
    let mut ticker = interval(Duration::from_millis(100));
    let mut last_event_us = 0u64;
    
    loop {
        ticker.tick().await;
        
        // Get new events
        let rocksdb = state.rocksdb.clone();
        let events = match tokio::task::spawn_blocking(move || {
            rocksdb.get_events_since(last_event_us)
        }).await {
            Ok(Ok(events)) => events,
            _ => break,
        };
        
        if !events.is_empty() {
            if let Some(last) = events.last() {
                last_event_us = last.timestamp_us;
            }
            
            let json = match serde_json::to_string(&events) {
                Ok(j) => j,
                Err(_) => break,
            };
            
            if socket.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
        
        // Periodically send active queries snapshot
        if ticker.period().as_millis() % 1000 == 0 {
            let rocksdb = state.rocksdb.clone();
            let queries = match tokio::task::spawn_blocking(move || {
                rocksdb.get_active_queries()
            }).await {
                Ok(Ok(q)) => q,
                _ => break,
            };
            
            let json = match serde_json::to_string(&queries) {
                Ok(j) => j,
                Err(_) => break,
            };
            
            if socket.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    }
}