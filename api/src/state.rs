use std::sync::Arc;
use parking_lot::RwLock;
use tokio_rusqlite::Connection as AsyncConnection;
use deadpool_postgres::Pool as PgPool;

use pg_rusty_proxy_shared::ProxyRocksDB;
use crate::services::FunctionCache;

#[derive(Clone)]
pub struct AppState {
    pub rocksdb: Arc<ProxyRocksDB>,
    pub sqlite: Arc<AsyncConnection>,
    pub pg_pool: Option<PgPool>,
    pub function_cache: Arc<FunctionCache>,
}

impl AppState {
    pub fn new(
        rocksdb: Arc<ProxyRocksDB>,
        sqlite: Arc<AsyncConnection>,
        pg_pool: Option<PgPool>,
    ) -> Self {
        Self {
            rocksdb,
            sqlite,
            pg_pool,
            function_cache: Arc::new(FunctionCache::new()),
        }
    }
}