use axum::{
    extract::{ws::WebSocket, ws::Message as WsMessage, WebSocketUpgrade, State, Path},
    response::{Html, IntoResponse},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use tracing::{error, info};
use std::process;

use super::active_queries::{ActiveQueries, ActiveQueryInfo};
use crate::database::queries::{
    get_detailed_blocking_info, get_transaction_history_from_db,
    get_blocker_transaction_details,
    DetailedBlockingInfo, TransactionHistoryResponse,
    BlockerTransactionInfo,
};

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
    
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
    
    loop {
        interval.tick().await;
        
        let top_queries = active_queries.get_top_running(10).await;
        
        let query_infos: Vec<ActiveQueryInfo> = top_queries
            .into_iter()
            .map(|q| q.into())
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

#[derive(Serialize)]
struct CancelResponse {
    success: bool,
    message: String,
}

async fn cancel_query_endpoint(
    Path(query_id): Path<u64>,
    State(active_queries): State<ActiveQueries>,
) -> Result<Json<CancelResponse>, StatusCode> {
    let query_exec = match active_queries.get_query(query_id).await {
        Some(q) => q,
        None => {
            return Ok(Json(CancelResponse {
                success: false,
                message: format!("Query {} not found or already completed", query_id),
            }));
        }
    };
    
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

async fn send_cancel_request(
    upstream_addr: std::net::SocketAddr,
    backend_pid: i32,
    secret_key: i32,
) -> tokio::io::Result<()> {
    use tokio::net::TcpStream;
    use tokio::io::AsyncWriteExt;
    
    let mut stream = TcpStream::connect(upstream_addr).await?;
    
    let cancel_request = [
        16i32.to_be_bytes(),
        80877102i32.to_be_bytes(),
        backend_pid.to_be_bytes(),
        secret_key.to_be_bytes(),
    ];
    
    for bytes in &cancel_request {
        stream.write_all(bytes).await?;
    }
    
    stream.shutdown().await?;
    
    info!(
        upstream = %upstream_addr,
        pid = backend_pid,
        "Cancel request sent"
    );
    
    Ok(())
}

async fn get_blocking_endpoint(
    Path(query_id): Path<u64>,
    State(active_queries): State<ActiveQueries>,
) -> Result<Json<DetailedBlockingInfo>, StatusCode> {
    use tracing::warn;
    
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

async fn get_transaction_history(
    Path(backend_pid): Path<i32>,
) -> Result<Json<TransactionHistoryResponse>, StatusCode> {
    match get_transaction_history_from_db(backend_pid).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            error!(error = %e, backend_pid = backend_pid, "Failed to get transaction history");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_blocker_transaction_endpoint(
    Path((query_id, blocker_pid)): Path<(u64, i32)>,
    State(active_queries): State<ActiveQueries>,
) -> Result<Json<BlockerTransactionInfo>, StatusCode> {
    use tracing::warn;
    
    // Получаем информацию о запросе чтобы найти upstream_addr
    let query_exec = match active_queries.get_query(query_id).await {
        Some(q) => q,
        None => {
            warn!(query_id = query_id, "Query not found");
            return Err(StatusCode::NOT_FOUND);
        }
    };
    
    let blocker_info = match get_blocker_transaction_details(
        query_exec.server_addr,
        blocker_pid,
        10, // последние 10 запросов
    ).await {
        Ok(info) => info,
        Err(e) => {
            error!(
                query_id = query_id,
                blocker_pid = blocker_pid,
                error = %e,
                "Failed to get blocker transaction details"
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    
    Ok(Json(blocker_info))
}

pub async fn start_http_server(active_queries: ActiveQueries) {
    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/api/stats/live", get(websocket_handler))
        .route("/api/query/:query_id/cancel", post(cancel_query_endpoint))
        .route("/api/query/:query_id/blocking", get(get_blocking_endpoint))
        .route("/api/transaction/:backend_pid/history", get(get_transaction_history))
		.route("/api/query/:query_id/blocker/:blocker_pid/transaction", get(get_blocker_transaction_endpoint)) 
        .with_state(active_queries);
    
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap_or_else(|e| fatal!(e, "Failed to bind HTTP server", addr = "127.0.0.1:8080"));
    
    info!("Dashboard available at http://127.0.0.1:8080");
    
    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| fatal!(e, "HTTP server failed"));
}