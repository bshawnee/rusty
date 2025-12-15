use clap::Parser;
use std::{env, net::SocketAddr};

#[derive(Parser, Debug, Clone)]
#[command(author = "Ilya Rozhnev",
          version =  env!("CARGO_PKG_VERSION"),
          about = env!("CARGO_PKG_NAME"),
          long_about = "Proxy server for PostgreSQL"
        )]
pub struct CliArguments {
	/// address of database server. Examle: 'localhost:5432'
	#[arg(short = 'u', long)]
	pub upstream: SocketAddr,
	/// address to listen for incoming connections. Example: localhost:5433'
	#[arg(short = 'd', long)]
	pub downstream: SocketAddr,
}