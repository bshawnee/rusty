pub mod cli;
pub mod proxy;
pub mod protocol;
pub mod monitoring;
pub mod database;
pub mod utils;

// Реэкспорт основных типов для удобства
pub use protocol::{TrafficDirection, TrafficData};
pub use proxy::start_proxy_server;
pub use monitoring::ActiveQueries;