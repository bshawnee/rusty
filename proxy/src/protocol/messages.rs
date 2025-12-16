use super::types::QueryType;

/// Парсит C-строку (заканчивается нулевым байтом)
pub fn parse_cstring(data: &[u8]) -> Option<(&str, &[u8])> {
    let null_pos = data.iter().position(|&b| b == 0)?;
    let string = std::str::from_utf8(&data[..null_pos]).ok()?;
    let rest = &data[null_pos + 1..];
    Some((string, rest))
}

/// Определяет тип запроса по первому ключевому слову
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
        "explain" | "analyze" => QueryType::Explain,
        "vacuum" | "reindex" | "cluster" => QueryType::Maintenance,
        "copy" => QueryType::Copy,
        "set" | "show" | "reset" => QueryType::Session,
        _ => QueryType::Other(first_word.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cstring() {
        let data = b"hello\0world\0";
        let (s1, rest) = parse_cstring(data).unwrap();
        assert_eq!(s1, "hello");
        
        let (s2, rest2) = parse_cstring(rest).unwrap();
        assert_eq!(s2, "world");
        assert_eq!(rest2.len(), 0);
    }

    #[test]
    fn test_detect_query_type() {
        assert!(matches!(detect_query_type("SELECT * FROM users"), QueryType::Select));
        assert!(matches!(detect_query_type("  select id from t"), QueryType::Select));
        assert!(matches!(detect_query_type("INSERT INTO users VALUES (1)"), QueryType::Insert));
        assert!(matches!(detect_query_type("UPDATE users SET x=1"), QueryType::Update));
        assert!(matches!(detect_query_type("DELETE FROM users"), QueryType::Delete));
        assert!(matches!(detect_query_type("BEGIN"), QueryType::Transaction));
        assert!(matches!(detect_query_type("COMMIT"), QueryType::Transaction));
        
        match detect_query_type("CUSTOM_CMD") {
            QueryType::Other(s) => assert_eq!(s, "custom_cmd"),
            _ => panic!("Expected Other"),
        }
    }
}