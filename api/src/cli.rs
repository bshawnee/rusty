use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    author = "Your Name",
    version = env!("CARGO_PKG_VERSION"),
    about = "PostgreSQL Proxy API Server",
    long_about = "Backend API server for PostgreSQL Proxy - provides REST API and WebSocket streaming"
)]
pub struct CliArguments {
    /// API server listen address
    /// 
    /// Example: 127.0.0.1:8080
    #[arg(short = 'a', long, env = "API_ADDRESS", default_value = "127.0.0.1:8080")]
    pub api_address: SocketAddr,

    /// Data directory (where RocksDB and SQLite are located)
    /// 
    /// Example: ./data
    #[arg(short = 'd', long, env = "DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    /// PostgreSQL URL for analysis queries (optional)
    /// 
    /// Example: postgres://user:password@localhost:5432/postgres
    #[arg(short = 'p', long, env = "POSTGRES_URL")]
    pub postgres_url: Option<String>,
}