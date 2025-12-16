use super::types::QueryType;

/// Парсит C-строку (заканчивается нулевым байтом)
pub fn parse_cstring(data: &[u8]) -> Option<(&str, &[u8])> {
    let null_pos = data.iter().position(|&b| b == 0)?;
    let string = std::str::from_utf8(&data[..null_pos]).ok()?;
    let rest = &data[null_pos + 1..];
    Some((string, rest))
}

pub fn detect_query_type(query: &str) -> QueryType {
    let query_lower = query.trim().to_lowercase();
    let first_word = query_lower.split_whitespace().next().unwrap_or("");
    
    match first_word {
        "select" | "with" => QueryType::Select,
        "insert" => QueryType::Insert,
        "update" => QueryType::Update,
        "delete" => QueryType::Delete,
        "create" => QueryType::Create,
        "drop" => QueryType::Drop,
        "alter" => QueryType::Alter,
        "begin" | "start" | "commit" | "rollback" | "savepoint" => QueryType::Transaction,
        _ => QueryType::Other(first_word.to_string()),
    }
}