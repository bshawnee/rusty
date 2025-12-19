mod history;
mod live;

use axum::{
    Router,
    routing::get,
};
use tower_http::{
    cors::CorsLayer,
    services::ServeDir,
};

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        // API routes
        .route("/health", get(health_check))
        .route("/api/live/queries", get(live::get_active_queries))
        .route("/api/live/events", get(live::get_events))
        .route("/api/live/stream", get(live::websocket_handler))
        .route("/api/live/metrics", get(live::get_metrics))
        .route("/api/history", get(history::get_query_history))
        .route("/api/history/:query_id", get(history::get_query_detail))
        
        // Static files (dashboard)
        .fallback_service(ServeDir::new("api/static"))
        
        // CORS
        .layer(CorsLayer::permissive())
        
        // State
        .with_state(state)
}

async fn health_check() -> &'static str {
    "OK"
}