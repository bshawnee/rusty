use clap::Parser;
use pg_rusty_proxy::{cli, proxy, monitoring};
use tracing_subscriber;
use tokio::sync::mpsc;
use std::process;

macro_rules! fatal {
    ($err:expr, $msg:literal) => {{
        tracing::error!(fatal = true, error = %$err, $msg);
        process::exit(1);
    }};
    ($err:expr, $msg:literal, $($key:tt = $val:expr),+) => {{
        tracing::error!(fatal = true, error = %$err, $($key = %$val),+, $msg);
        process::exit(1);
    }};
}

#[tokio::main]
async fn main() {
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

    let cli_args = cli::CliArguments::parse();

    tracing::info!(
        downstream = %cli_args.downstream,
        upstream = %cli_args.upstream,
        "Starting proxy server"
    );

    // Инициализация компонентов
    let (log_tx, log_rx) = mpsc::channel(10000);
    
    let log_writer = monitoring::LogWriter::new("proxy_logs.db", log_rx)
        .await
        .unwrap_or_else(|e| fatal!(e, "Failed to initialize SQLite database"));
    
    tokio::spawn(async move {
        log_writer.run().await;
    });

    let active_queries = monitoring::ActiveQueries::new();

    // HTTP сервер
    let active_queries_http = active_queries.clone();
    tokio::spawn(async move {
        monitoring::start_http_server(active_queries_http).await;
    });

    // Запуск прокси
    if let Err(e) = proxy::start_proxy_server(
        cli_args.downstream,
        cli_args.upstream,
        active_queries,
        log_tx,
    ).await {
        fatal!(e, "Proxy server failed");
    }
}