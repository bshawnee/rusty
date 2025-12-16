// SQLite schema (как было)
pub const DB_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS query_logs (
    query_id TEXT PRIMARY KEY,
    session_id INTEGER NOT NULL,
    backend_pid INTEGER,
    client_addr TEXT NOT NULL,
    server_addr TEXT NOT NULL,
    user_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    application_name TEXT,
    query_text TEXT NOT NULL,
    query_type TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    execution_time_us INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000000)
);

CREATE INDEX IF NOT EXISTS idx_query_logs_backend_pid ON query_logs(backend_pid);
CREATE INDEX IF NOT EXISTS idx_query_logs_session_id ON query_logs(session_id);
CREATE INDEX IF NOT EXISTS idx_query_logs_start_time ON query_logs(start_time);
CREATE INDEX IF NOT EXISTS idx_query_logs_user ON query_logs(user_name);
CREATE INDEX IF NOT EXISTS idx_query_logs_status ON query_logs(status);
"#;