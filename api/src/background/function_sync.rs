use tokio::time::{interval, Duration};
use tracing::{error, info};

use crate::state::AppState;
use crate::services::{FunctionInfo, Volatility};

pub fn start_function_sync(state: AppState) {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3600)); // Every hour
        
        // Run immediately on startup
        if let Err(e) = sync_functions(&state).await {
            error!(error = %e, "Initial function sync failed");
        }
        
        loop {
            interval.tick().await;
            
            match sync_functions(&state).await {
                Ok(count) => {
                    info!(count, "Synced immutable functions from PostgreSQL");
                }
                Err(e) => {
                    error!(error = %e, "Failed to sync functions");
                }
            }
        }
    });
}

async fn sync_functions(state: &AppState) -> Result<usize, Box<dyn std::error::Error>> {
    let pg_pool = match &state.pg_pool {
        Some(pool) => pool,
        None => return Ok(0),
    };
    
    let client = pg_pool.get().await?;
    
    let rows = client.query(
        r#"
        SELECT 
            p.oid::int4 as oid,
            n.nspname as schema,
            p.proname as name,
            p.provolatile as volatility
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE p.provolatile = 'i'  -- Only IMMUTABLE functions
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, p.proname
        "#,
        &[]
    ).await?;
    
    let mut functions = Vec::new();
    
    for row in rows {
        let oid: i32 = row.get("oid");
        let schema: String = row.get("schema");
        let name: String = row.get("name");
        let volatility_char: String = row.get("volatility");
        
        let volatility = match volatility_char.as_str() {
            "i" => Volatility::Immutable,
            "s" => Volatility::Stable,
            "v" => Volatility::Volatile,
            _ => Volatility::Volatile,
        };
        
        functions.push(FunctionInfo {
            oid: oid as u32,
            schema,
            name,
            volatility,
            last_checked: std::time::SystemTime::now(),
        });
    }
    
    let count = functions.len();
    state.function_cache.update(functions);
    
    Ok(count)
}
