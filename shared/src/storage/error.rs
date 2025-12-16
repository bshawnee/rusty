use thiserror::Error;

#[derive(Error, Debug)]
pub enum RocksDBError {
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    
    #[error("Not found: {0}")]
    NotFound(String),
    
    #[error("Invalid data: {0}")]
    InvalidData(String),
}

pub type Result<T> = std::result::Result<T, RocksDBError>;