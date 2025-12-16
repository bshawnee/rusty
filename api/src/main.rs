use clap::Parser;
use tracing::{error, info};
use std::sync::Arc;
use std::process;
use std::path::Path;

mod cli;
mod state;
mod routes;
mod services;
mod background;

use state::AppState;

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
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    let cli_args = cli::CliArguments::parse();

    info!(
        api_address = %cli_args.api_address,
        data_dir = %cli_args.data_dir.display(),
        "Starting API server"
    );

    // Open RocksDB (read-only)
    let rocksdb_path = cli_args.data_dir.join("live.rocksdb");
    let rocksdb = Arc::new(
        pg_rusty_proxy_shared::ProxyRocksDB::open_readonly(&rocksdb_path)
            .unwrap_or_else(|e| fatal!(e, "Failed to open RocksDB", path = rocksdb_path.display()))
    );
    
    info!(path = %rocksdb_path.display(), "RocksDB opened (read-only)");

    // Open SQLite (read-only)
    let sqlite_path = cli_args.data_dir.join("query_logs.db");
    let sqlite = tokio_rusqlite::Connection::open(sqlite_path.to_str().unwrap())
        .await
        .unwrap_or_else(|e| fatal!(e, "Failed to open SQLite", path = sqlite_path.display()));
    
    info!(path = %sqlite_path.display(), "SQLite opened");

    // PostgreSQL connection pool
    let pg_pool = if let Some(ref pg_url) = cli_args.postgres_url {
        let pool = deadpool_postgres::Pool::builder(
            deadpool_postgres::Manager::new(
                pg_url.parse().unwrap_or_else(|e| fatal!(e, "Invalid PostgreSQL URL")),
                tokio_postgres::NoTls,
            )
        )
        .max_size(5)
        .build()
        .unwrap_or_else(|e| fatal!(e, "Failed to create PostgreSQL pool"));
        
        info!(url = %pg_url, "PostgreSQL pool created");
        Some(pool)
    } else {
        info!("PostgreSQL pool not configured");
        None
    };

    // Create app state
    let state = AppState::new(
        rocksdb,
        Arc::new(sqlite),
        pg_pool,
    );

    // Start background tasks
    if state.pg_pool.is_some() {
        background::start_function_sync(state.clone());
        info!("Function sync background task started");
    }

    // Build routes
    let app = routes::build_router(state);

    // Start server
    let listener = tokio::net::TcpListener::bind(&cli_args.api_address)
        .await
        .unwrap_or_else(|e| fatal!(e, "Failed to bind", address = cli_args.api_address));

    info!(address = %cli_args.api_address, "API server listening");

    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| fatal!(e, "Server failed"));
}