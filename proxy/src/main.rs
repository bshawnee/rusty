use clap::Parser;
use pg_rusty_proxy_shared::{ProxyRocksDB, DB_SCHEMA};
use tokio::sync::mpsc;
use tokio_rusqlite::Connection as AsyncConnection;
use tracing::{error, info};
use std::process;
use std::path::Path;
use std::sync::Arc;

mod cli;
mod proxy;
mod protocol;
mod monitoring;

use cli::CliArguments;
use monitoring::{LogWriter, QueryLogEntry};

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
        .with_line_number(false)
        .init();

    let cli_args = CliArguments::parse();

    info!(
        downstream = %cli_args.downstream,
        upstream = %cli_args.upstream,
        "Starting PostgreSQL proxy"
    );

    // Create data directory if not exists
    let data_dir = Path::new("./data");
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)
            .unwrap_or_else(|e| fatal!(e, "Failed to create data directory"));
    }

    // ==================== RocksDB (Live Data) ====================
    
    let rocksdb_path = data_dir.join("live.rocksdb");
    let rocksdb = Arc::new(
        ProxyRocksDB::open(&rocksdb_path)
            .unwrap_or_else(|e| fatal!(e, "Failed to open RocksDB", path = rocksdb_path.display()))
    );
    
    info!(path = %rocksdb_path.display(), "RocksDB opened");

    // Background task: cleanup old events from RocksDB
    let rocksdb_cleanup = rocksdb.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 minutes
        
        loop {
            interval.tick().await;
            
            match rocksdb_cleanup.compact_events(std::time::Duration::from_secs(3600)) {
                Ok(deleted) => {
                    if deleted > 0 {
                        info!(deleted, "Compacted old events from RocksDB");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to compact events");
                }
            }
        }
    });

    // ==================== SQLite (Historical Logs) ====================
    
    let (log_tx, log_rx) = mpsc::channel(10_000);
    
    let sqlite_path = data_dir.join("query_logs.db");
    let log_writer = LogWriter::new(sqlite_path.to_str().unwrap(), log_rx)
        .await
        .unwrap_or_else(|e| fatal!(e, "Failed to initialize SQLite", path = sqlite_path.display()));
    
    info!(path = %sqlite_path.display(), "SQLite initialized");
    
    tokio::spawn(async move {
        log_writer.run().await;
    });

    // ==================== Start Proxy Server ====================
    
    if let Err(e) = proxy::start_proxy_server(
        cli_args.downstream,
        cli_args.upstream,
        rocksdb,
        log_tx,
    ).await {
        fatal!(e, "Proxy server failed");
    }
}