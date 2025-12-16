use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use std::net::SocketAddr;
use std::sync::Arc;

use pg_rusty_proxy_shared::ProxyRocksDB;
use crate::protocol::{TrafficData, TrafficDirection};
use crate::monitoring::QueryLogEntry;
use super::parser::PostgresParser;

pub async fn start_proxy_server(
    downstream: SocketAddr,
    upstream: SocketAddr,
    rocksdb: Arc<ProxyRocksDB>,
    log_tx: mpsc::Sender<QueryLogEntry>,
) -> io::Result<()> {
    let listener = TcpListener::bind(downstream).await?;
    info!(address = %downstream, "Proxy server listening");

    let mut session_counter = 0u64;

    loop {
        let (client_stream, client_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!(error = %e, "Failed to accept connection");
                continue;
            }
        };

        debug!(client = %client_addr, "New connection accepted");
        
        session_counter += 1;
        let session_id = session_counter;
        
        let rocksdb_clone = rocksdb.clone();
        let log_tx_clone = log_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(
                client_stream,
                upstream,
                client_addr,
                rocksdb_clone,
                session_id,
                log_tx_clone,
            ).await {
                error!(client = %client_addr, error = %e, "Error handling client");
            }
        });
    }
}

async fn handle_client(
    client: TcpStream,
    upstream_addr: SocketAddr,
    client_addr: SocketAddr,
    rocksdb: Arc<ProxyRocksDB>,
    session_id: u64,
    log_tx: mpsc::Sender<QueryLogEntry>,
) -> io::Result<()> {
    let server = TcpStream::connect(upstream_addr).await.map_err(|e| {
        error!(
            client = %client_addr,
            upstream = %upstream_addr,
            error = %e,
            "Failed to connect to upstream"
        );
        e
    })?;

    info!(
        client = %client_addr,
        upstream = %upstream_addr,
        session_id = session_id,
        "Connected to upstream"
    );
    
    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();

    let (tx, rx) = mpsc::channel::<TrafficData>(100);
    
    // Spawn parser task
    let parser_task = tokio::spawn(async move {
        traffic_parser(rx, rocksdb, upstream_addr, client_addr, session_id, log_tx).await;
    });

    let tx_c2s = tx.clone();
    let tx_s2c = tx;
    
    // Client -> Server
    let client_to_server = tokio::spawn(async move {
        copy_with_logging(
            &mut client_read,
            &mut server_write,
            tx_c2s,
            TrafficDirection::ClientToServer,
            client_addr,
        ).await
    });

    // Server -> Client
    let server_to_client = tokio::spawn(async move {
        copy_with_logging(
            &mut server_read,
            &mut client_write,
            tx_s2c,
            TrafficDirection::ServerToClient,
            client_addr,
        ).await
    });

    // Wait for both directions to complete
    let _ = tokio::try_join!(client_to_server, server_to_client);
    
    info!(
        client = %client_addr,
        session_id = session_id,
        "Connection closed"
    );
    
    drop(parser_task);

    Ok(())
}

async fn traffic_parser(
    mut rx: mpsc::Receiver<TrafficData>,
    rocksdb: Arc<ProxyRocksDB>,
    server_addr: SocketAddr,
    client_addr: SocketAddr,
    session_id: u64,
    log_tx: mpsc::Sender<QueryLogEntry>,
) {
    let mut parser = PostgresParser::new(
        client_addr,
        server_addr,
        rocksdb,
        session_id,
        log_tx,
    );
    
    while let Some(traffic) = rx.recv().await {
        match traffic.direction {
            TrafficDirection::ClientToServer => {
                parser.parse_client_message(&traffic.data);
            }
            TrafficDirection::ServerToClient => {
                parser.parse_server_message(&traffic.data);
            }
        }
    }
    
    debug!(
        client = %client_addr,
        session_id = session_id,
        "Traffic parser stopped"
    );
}

async fn copy_with_logging<R, W>(
    reader: &mut R,
    writer: &mut W,
    tx: mpsc::Sender<TrafficData>,
    direction: TrafficDirection,
    client_addr: SocketAddr,
) -> io::Result<u64>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buffer = vec![0u8; 8192];
    let mut total_bytes = 0u64;
    
    loop {
        let n = reader.read(&mut buffer).await?;
        
        if n == 0 {
            break;
        }
        
        total_bytes += n as u64;
        
        // Forward data immediately
        writer.write_all(&buffer[..n]).await?;
        
        // Send to parser (non-blocking)
        let traffic = TrafficData {
            direction: direction.clone(),
            client_addr,
            data: buffer[..n].to_vec(),
        };
        
        let _ = tx.try_send(traffic);
    }
    
    Ok(total_bytes)
}