use clap::Parser;
use std::net::SocketAddr;

#[derive(Parser, Debug, Clone)]
#[command(
    author = "Ilya Rozhnev",
    version = env!("CARGO_PKG_VERSION"),
    about = "PostgreSQL Proxy Server",
    long_about = "A high-performance PostgreSQL proxy with query monitoring, caching, and blocking analysis"
)]
pub struct CliArguments {
    /// PostgreSQL server address (upstream)
    /// 
    /// Example: localhost:5432
    #[arg(short = 'u', long, env = "PROXY_UPSTREAM")]
    pub upstream: SocketAddr,

    /// Proxy listen address (downstream)
    /// 
    /// Example: localhost:5433
    #[arg(short = 'd', long, env = "PROXY_DOWNSTREAM")]
    pub downstream: SocketAddr,
}