use parking_lot::RwLock;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Volatility {
    Immutable,  // 'i'
    Stable,     // 's'
    Volatile,   // 'v'
}

#[derive(Clone, Debug, Serialize)]
pub struct FunctionInfo {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub volatility: Volatility,
    pub last_checked: SystemTime,
}

pub struct FunctionCache {
    // schema.function_name -> FunctionInfo
    cache: RwLock<HashMap<String, FunctionInfo>>,
}

impl FunctionCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }
    
    pub fn get(&self, schema: &str, name: &str) -> Option<FunctionInfo> {
        let key = format!("{}.{}", schema, name);
        let cache = self.cache.read();
        cache.get(&key).cloned()
    }
    
    pub fn get_by_name(&self, name: &str) -> Option<FunctionInfo> {
        let cache = self.cache.read();
        
        // Search in public schema first
        if let Some(info) = cache.get(&format!("public.{}", name)) {
            return Some(info.clone());
        }
        
        // Search in all schemas
        for (key, info) in cache.iter() {
            if info.name == name {
                return Some(info.clone());
            }
        }
        
        None
    }
    
    pub fn update(&self, functions: Vec<FunctionInfo>) {
        let mut cache = self.cache.write();
        cache.clear();
        
        for func in functions {
            let key = format!("{}.{}", func.schema, func.name);
            cache.insert(key, func);
        }
    }
    
    pub fn get_all(&self) -> Vec<FunctionInfo> {
        let cache = self.cache.read();
        cache.values().cloned().collect()
    }
    
    pub fn count(&self) -> usize {
        let cache = self.cache.read();
        cache.len()
    }
    
    pub fn is_cacheable(&self, name: &str) -> bool {
        match self.get_by_name(name) {
            Some(info) => matches!(info.volatility, Volatility::Immutable),
            None => false, // Unknown = not cacheable (fail-safe)
        }
    }
}