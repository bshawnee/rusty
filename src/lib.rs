pub mod cli;

#[derive(Debug, Clone)]
pub enum TrafficDirection {
    ClientToServer,
    ServerToClient,
}

#[derive(Debug)]
pub struct TrafficData {
    pub direction: TrafficDirection,
    pub client_addr: std::net::SocketAddr,
    pub data: Vec<u8>,
}