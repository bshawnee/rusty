pub const DB_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS query_logs (
    -- Уникальный ID (UUID v7 - timestamped, sortable)
    query_id TEXT PRIMARY KEY,
    
    -- Идентификаторы сессии/соединения
    session_id INTEGER NOT NULL,
    backend_pid INTEGER,
    client_addr TEXT NOT NULL,
    server_addr TEXT NOT NULL,
    
    -- Информация о подключении
    user_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    application_name TEXT,
    
    -- Сам запрос
    query_text TEXT NOT NULL,
    query_type TEXT NOT NULL,
    
    -- Время выполнения
    start_time INTEGER NOT NULL,  -- Unix timestamp в микросекундах
    execution_time_us INTEGER NOT NULL,  -- Микросекунды
    
    -- Статус
    status TEXT NOT NULL,  -- 'success', 'error', 'cancelled'
    error_message TEXT,
    
    -- Метаданные
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000000)
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_query_logs_backend_pid ON query_logs(backend_pid);
CREATE INDEX IF NOT EXISTS idx_query_logs_session_id ON query_logs(session_id);
CREATE INDEX IF NOT EXISTS idx_query_logs_start_time ON query_logs(start_time);
CREATE INDEX IF NOT EXISTS idx_query_logs_user ON query_logs(user_name);
CREATE INDEX IF NOT EXISTS idx_query_logs_status ON query_logs(status);

-- Таблица для активных транзакций
CREATE TABLE IF NOT EXISTS active_transactions (
    backend_pid INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    user_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    application_name TEXT,
    client_addr TEXT NOT NULL,
    server_addr TEXT NOT NULL,
    transaction_start INTEGER NOT NULL,  -- Unix timestamp в микросекундах
    last_query_id TEXT,
    last_update INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_active_tx_session ON active_transactions(session_id);
"#;