use std::fs::File;
use std::io::{BufRead, BufReader, Lines};
use std::iter::FusedIterator;
use std::path::Path;

use anyhow::Error;
use chrono::{DateTime, Utc};

#[derive(Debug, PartialEq, Eq)]
pub struct CapturedQuery {
    pub timestamp: DateTime<Utc>,
    pub connection_id: u64,
    pub command_type: CommandType,
    pub sql: String,
    pub query_type: QueryType,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommandType {
    Query,
    Connect,
    Quit,
    Prepare,
    Execute,
    Other(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    DDL,
    Administrative,
    Other,
}

/// A pending captured query is a query that is being captured but has not yet been finalized.
/// It contains the timestamp, connection ID, command type, and SQL of the query.
/// It is used to accumulate the SQL of a query that spans multiple lines.
struct PendingCapturedQuery {
    timestamp: DateTime<Utc>,
    connection_id: u64,
    command_type: CommandType,
    sql: String,
}

/// Configures which captured queries should be yielded by the streaming filter layer.
#[derive(Debug, Default)]
pub struct CapturedQueryFilter {
    pub allowed_query_types: Option<Vec<QueryType>>,
    pub connection_id: Option<u64>,
    pub skip_noisy_queries: bool,
}

impl CapturedQueryFilter {
    fn matches(&self, query: &CapturedQuery) -> bool {
        self
            .allowed_query_types
            .as_ref()
            .map(|allowed_query_types| allowed_query_types.contains(&query.query_type))
            .unwrap_or(true)
            && self
                .connection_id
                .map(|expected_connection_id| query.connection_id == expected_connection_id)
                .unwrap_or(true)
            && (!self.skip_noisy_queries || !is_noisy_query(&query.sql))
    }
}

/// Streams captured queries from a general log reader without buffering the full file.
pub struct CapturedQueryStream<R: BufRead> {
    lines: Lines<R>,
    current_query: Option<PendingCapturedQuery>,
    pending_blank_lines: usize,
    finished: bool,
}

/// Streams captured queries that match a filter without buffering the full file.
pub struct FilteredCapturedQueryStream<I> {
    inner: I,
    filter: CapturedQueryFilter,
}

impl<R: BufRead> CapturedQueryStream<R> {
    pub fn new(reader: R) -> Self {
        Self {
            lines: reader.lines(),
            current_query: None,
            pending_blank_lines: 0,
            finished: false,
        }
    }

    pub fn with_filter(self, filter: CapturedQueryFilter) -> FilteredCapturedQueryStream<Self> {
        FilteredCapturedQueryStream::new(self, filter)
    }

    fn process_line(&mut self, line: &str) -> Option<CapturedQuery> {
        if line.is_empty() {
            if self.current_query.is_some() {
                self.pending_blank_lines += 1;
            }
            return None;
        }

        if let Some(next_query) = parse_log_entry(line) {
            self.pending_blank_lines = 0;
            return self.current_query.replace(next_query).map(finalize_query);
        }

        if looks_like_log_entry_start(line) {
            self.pending_blank_lines = 0;
            return None;
        }

        if let Some(existing_query) = self.current_query.as_mut() {
            for _ in 0..self.pending_blank_lines {
                existing_query.sql.push('\n');
            }
            self.pending_blank_lines = 0;
            push_continuation_line(existing_query, line);
        }

        None
    }

    fn finish(&mut self) -> Option<CapturedQuery> {
        self.finished = true;
        self.pending_blank_lines = 0;
        self.current_query.take().map(finalize_query)
    }
}

impl<R: BufRead> Iterator for CapturedQueryStream<R> {
    type Item = Result<CapturedQuery, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            match self.lines.next() {
                Some(Ok(line)) => {
                    if let Some(query) = self.process_line(line.trim_end()) {
                        return Some(Ok(query));
                    }
                }
                Some(Err(error)) => {
                    self.finished = true;
                    self.current_query = None;
                    return Some(Err(error.into()));
                }
                None => return self.finish().map(Ok),
            }
        }
    }
}

impl<R: BufRead> FusedIterator for CapturedQueryStream<R> {}

impl<I> FilteredCapturedQueryStream<I> {
    pub fn new(inner: I, filter: CapturedQueryFilter) -> Self {
        Self { inner, filter }
    }
}

impl<I> Iterator for FilteredCapturedQueryStream<I>
where
    I: Iterator<Item = Result<CapturedQuery, Error>>,
{
    type Item = Result<CapturedQuery, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Ok(query) if self.filter.matches(&query) => return Some(Ok(query)),
                Ok(_) => continue,
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

impl<I> FusedIterator for FilteredCapturedQueryStream<I> where
    I: FusedIterator<Item = Result<CapturedQuery, Error>>
{
}

// TODO: Add a structured warning/error path so skipped lines are observable instead of silently ignored.

/// Opens a general log file and streams captured queries one at a time.
pub fn capture_general_log(path: &Path) -> Result<CapturedQueryStream<BufReader<File>>, Error> {
    let file = File::open(path)?;
    Ok(CapturedQueryStream::new(BufReader::new(file)))
}

fn push_continuation_line(query: &mut PendingCapturedQuery, line: &str) {
    if !query.sql.is_empty() {
        query.sql.push('\n');
    }
    query.sql.push_str(line);
}

/// Parses a log entry and returns a pending captured query.
fn parse_log_entry(line: &str) -> Option<PendingCapturedQuery> {
    let first_whitespace = line.find(char::is_whitespace)?;
    let timestamp = DateTime::parse_from_rfc3339(&line[..first_whitespace])
        .ok()?
        .with_timezone(&Utc);

    let remaining = line[first_whitespace..].trim_start();
    let second_whitespace = remaining.find(char::is_whitespace)?;
    let connection_id = remaining[..second_whitespace].parse().ok()?;

    let remaining = remaining[second_whitespace..].trim_start();
    let (command_type, sql) = match remaining.find(char::is_whitespace) {
        Some(index) => (&remaining[..index], remaining[index..].trim_start()),
        None => (remaining, ""),
    };

    Some(PendingCapturedQuery {
        timestamp,
        connection_id,
        command_type: parse_command_type(command_type),
        sql: sql.to_string(),
    })
}

fn looks_like_log_entry_start(line: &str) -> bool {
    let Some((timestamp, remaining)) = split_once_whitespace(line) else {
        return false;
    };

    if !looks_like_rfc3339_prefix(timestamp) {
        return false;
    }

    let Some((connection_id, _)) = split_once_whitespace(remaining.trim_start()) else {
        return false;
    };

    !connection_id.is_empty() && connection_id.chars().all(|character| character.is_ascii_digit())
}

fn split_once_whitespace(input: &str) -> Option<(&str, &str)> {
    let whitespace_index = input.find(char::is_whitespace)?;
    Some((&input[..whitespace_index], &input[whitespace_index..]))
}

fn looks_like_rfc3339_prefix(token: &str) -> bool {
    let bytes = token.as_bytes();
    bytes.len() >= 20
        && bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && bytes.get(10) == Some(&b'T')
        && bytes.get(13) == Some(&b':')
        && bytes.get(16) == Some(&b':')
}

/// Finalizes a pending captured query and returns a captured query.
fn finalize_query(query: PendingCapturedQuery) -> CapturedQuery {
    CapturedQuery {
        timestamp: query.timestamp,
        connection_id: query.connection_id,
        query_type: classify_query_type(&query.command_type, &query.sql),
        command_type: query.command_type,
        sql: query.sql,
    }
}

/// Parse a command type from a string and return a CommandType.
fn parse_command_type(command_type: &str) -> CommandType {
    match command_type {
        "Query" => CommandType::Query,
        "Connect" => CommandType::Connect,
        "Quit" => CommandType::Quit,
        "Prepare" => CommandType::Prepare,
        "Execute" => CommandType::Execute,
        other => CommandType::Other(other.to_string()),
    }
}

/// Classifies a query type based on the command type and SQL.
fn classify_query_type(command_type: &CommandType, sql: &str) -> QueryType {
    match command_type {
        CommandType::Connect | CommandType::Quit => return QueryType::Administrative,
        _ => {}
    }

    let keywords = normalized_leading_keywords(sql, 2);

    match keywords.as_slice() {
        [first, second] if first == "DROP" && second == "PREPARE" => {
            return QueryType::Administrative;
        }
        _ => {}
    }

    let first_keyword = keywords.first().map(String::as_str).unwrap_or("");

    match first_keyword {
        "SELECT" => QueryType::Select,
        "INSERT" | "REPLACE" => QueryType::Insert,
        "UPDATE" => QueryType::Update,
        "DELETE" => QueryType::Delete,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME" => QueryType::DDL,
        "SET" | "USE" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "CALL" | "DO"
        | "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "LOCK" | "UNLOCK" | "PREPARE"
        | "EXECUTE" | "DEALLOCATE" | "GRANT" | "REVOKE" => {
            QueryType::Administrative
        }
        "" => QueryType::Other,
        _ => QueryType::Other,
    }
}

fn is_noisy_query(sql: &str) -> bool {
    match normalized_leading_keywords(sql, 2).as_slice() {
        [first, second] if first == "SET" && second == "NAMES" => true,
        [first, second] if first == "SELECT" && second == "VERSION" => true,
        _ => false,
    }
}

fn normalized_leading_keywords(sql: &str, count: usize) -> Vec<String> {
    sql.split_whitespace()
        .take(count)
        .map(normalize_keyword)
        .collect()
}

/// Normalizes a keyword by removing non-alphanumeric characters and converting to uppercase.
fn normalize_keyword(token: &str) -> String {
    token
        .trim_matches(|character: char| !character.is_ascii_alphabetic())
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn capture_fixture(fixture: &str) -> Vec<CapturedQuery> {
        CapturedQueryStream::new(Cursor::new(fixture))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn capture_filtered_fixture(
        fixture: &str,
        allowed_query_types: Vec<QueryType>,
        connection_id: Option<u64>,
    ) -> Vec<CapturedQuery> {
        CapturedQueryStream::new(Cursor::new(fixture))
            .with_filter(CapturedQueryFilter {
                allowed_query_types: Some(allowed_query_types),
                connection_id,
                skip_noisy_queries: true,
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn sqls(queries: &[CapturedQuery]) -> Vec<&str> {
        queries.iter().map(|query| query.sql.as_str()).collect()
    }

    #[test]
    fn test_capture_general_log() {
        let path = Path::new("test-data/general.log");
        let queries = capture_general_log(path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(queries.len(), 13_954);

        let first_query = &queries[0];
        assert_eq!(
            first_query.timestamp,
            DateTime::parse_from_rfc3339("2026-03-06T18:26:16.879568Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(first_query.connection_id, 1);
        assert_eq!(first_query.command_type, CommandType::Query);
        assert_eq!(first_query.sql, "CREATE DATABASE mysql;");
        assert_eq!(first_query.query_type, QueryType::DDL);

        let prepare_query = queries
            .iter()
            .find(|query| query.command_type == CommandType::Prepare)
            .unwrap();
        assert!(prepare_query.sql.contains("\n\tdatabase_name"));
        assert_eq!(prepare_query.query_type, QueryType::DDL);

        let quit_query = queries
            .iter()
            .find(|query| query.command_type == CommandType::Quit)
            .unwrap();
        assert!(quit_query.sql.is_empty());
        assert_eq!(quit_query.query_type, QueryType::Administrative);

        let describe_query = queries
            .iter()
            .find(|query| query.sql == "DESCRIBE users")
            .unwrap();
        assert_eq!(describe_query.query_type, QueryType::Administrative);
    }

    #[test]
    fn test_capture_mysql_8_general_log() {
        let path = Path::new("test-data/general_80.log");
        let queries = capture_general_log(path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(queries.len(), 18_424);

        let first_query = &queries[0];
        assert_eq!(
            first_query.timestamp,
            DateTime::parse_from_rfc3339("2026-03-06T21:41:23.470247Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(first_query.connection_id, 6);
        assert_eq!(first_query.command_type, CommandType::Query);
        assert_eq!(first_query.sql, "USE mysql;");
        assert_eq!(first_query.query_type, QueryType::Administrative);

        let copyright_query = queries
            .iter()
            .find(|query| query.sql.starts_with("-- Copyright (c) 2007, 2025, Oracle"))
            .unwrap();
        assert!(copyright_query.sql.contains("-- This program is free software;"));
        assert!(copyright_query.sql.contains("set @have_innodb="));

        let prepare_user_query = queries
            .iter()
            .find(|query| {
                query.command_type == CommandType::Prepare
                    && query.sql.starts_with("CREATE TABLE IF NOT EXISTS user\n(")
            })
            .unwrap();
        assert_eq!(prepare_user_query.connection_id, 6);
        assert_eq!(prepare_user_query.query_type, QueryType::DDL);
        assert!(prepare_user_query.sql.contains("User_attributes JSON DEFAULT NULL"));

        let repeated_header_execute = queries
            .iter()
            .find(|query| {
                query.connection_id == 0
                    && query.command_type == CommandType::Execute
                    && query
                        .sql
                        .starts_with("CREATE TABLE performance_schema.innodb_redo_log_files(")
            })
            .unwrap();
        assert_eq!(repeated_header_execute.query_type, QueryType::DDL);

        let last_query = queries.last().unwrap();
        assert_eq!(
            last_query.timestamp,
            DateTime::parse_from_rfc3339("2026-03-06T21:43:06.842038Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(last_query.connection_id, 10);
        assert_eq!(last_query.command_type, CommandType::Quit);
        assert!(last_query.sql.is_empty());
        assert_eq!(last_query.query_type, QueryType::Administrative);
    }

    #[test]
    fn classifies_admin_statements() {
        assert_eq!(
            classify_query_type(&CommandType::Query, "DROP PREPARE stmt;"),
            QueryType::Administrative
        );
        assert_eq!(
            classify_query_type(&CommandType::Query, "DESCRIBE users;"),
            QueryType::Administrative
        );
        assert_eq!(
            classify_query_type(&CommandType::Query, "DESC users;"),
            QueryType::Administrative
        );
        assert_eq!(
            classify_query_type(&CommandType::Query, "EXPLAIN SELECT * FROM users;"),
            QueryType::Administrative
        );
        assert_eq!(
            classify_query_type(&CommandType::Query, "DROP TABLE users;"),
            QueryType::DDL
        );
    }

    #[test]
    fn preserves_blank_lines_inside_multiline_queries() {
        let queries = capture_fixture(concat!(
            "2026-03-06T18:26:16.944052Z\t1 Query\tSET @cmd=\"line one\n",
            "\n",
            "line three\"\n",
            "\n",
            "2026-03-06T18:26:16.944065Z\t1 Query\tSELECT 1;\n",
            "\n",
        ));

        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].sql, "SET @cmd=\"line one\n\nline three\"");
        assert_eq!(queries[1].sql, "SELECT 1;");
    }

    #[test]
    fn streams_queries_incrementally() {
        let mut queries = CapturedQueryStream::new(Cursor::new(concat!(
            "2026-03-06T18:26:16.879568Z\t1 Query\tSELECT 1;\n",
            "2026-03-06T18:26:16.880000Z\t1 Query\tSELECT 2;\n",
        )));

        let first_query = queries.next().unwrap().unwrap();
        assert_eq!(first_query.sql, "SELECT 1;");

        let second_query = queries.next().unwrap().unwrap();
        assert_eq!(second_query.sql, "SELECT 2;");

        assert!(queries.next().is_none());
    }

    #[test]
    fn future_filter_layer_spec_skips_noise_and_optionally_scopes_connection_id() {
        let fixture = concat!(
            "2026-03-06T18:26:16.879568Z\t1 Query\tSET NAMES utf8mb4;\n",
            "2026-03-06T18:26:16.880000Z\t1 Query\tSELECT @@version;\n",
            "2026-03-06T18:26:16.881000Z\t1 Query\tSELECT * FROM users;\n",
            "2026-03-06T18:26:16.882000Z\t1 Query\tUPDATE users SET name = 'Ada';\n",
            "2026-03-06T18:26:16.883000Z\t2 Query\tSELECT * FROM accounts;\n",
            "2026-03-06T18:26:16.884000Z\t1 Query\tCREATE TABLE widgets (id INT);\n",
        );

        let allowed_query_types = [
            QueryType::Select,
            QueryType::DDL,
            QueryType::Administrative,
        ];

        let filtered_without_connection_scope =
            capture_filtered_fixture(fixture, allowed_query_types.into(), None);
        assert_eq!(
            sqls(&filtered_without_connection_scope),
            vec![
                "SELECT * FROM users;",
                "SELECT * FROM accounts;",
                "CREATE TABLE widgets (id INT);",
            ]
        );

        let filtered_for_connection_one =
            capture_filtered_fixture(
                fixture,
                vec![QueryType::Select, QueryType::DDL, QueryType::Administrative],
                Some(1),
            );
        assert_eq!(
            sqls(&filtered_for_connection_one),
            vec!["SELECT * FROM users;", "CREATE TABLE widgets (id INT);",]
        );
    }

    #[test]
    fn skips_malformed_header_like_lines_instead_of_appending_them() {
        let queries = capture_fixture(concat!(
            "2026-03-06T18:26:16.879568Z\t1 Query\tSELECT 1\n",
            "\n",
            "2026-03-06T18:26:xx.000000Z\t99 Query\tTHIS SHOULD BE SKIPPED\n",
            "2026-03-06T18:26:16.880000Z\t1 Query\tSELECT 2\n",
            "\n",
        ));

        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].sql, "SELECT 1");
        assert_eq!(queries[1].sql, "SELECT 2");
    }

    #[test]
    fn surfaces_non_utf8_binary_content_as_invalid_data() {
        let fixture = b"2026-03-06T18:26:16.879568Z\t1 Query\tSELECT 1;\n\
2026-03-06T18:26:16.880000Z\t1 Query\t\xff\xfe\n";
        let mut queries = CapturedQueryStream::new(Cursor::new(&fixture[..]));

        let error = queries.next().unwrap().unwrap_err();
        let io_error = error
            .downcast_ref::<std::io::Error>()
            .expect("expected non-UTF8 content to surface as an io::Error");

        assert_eq!(io_error.kind(), std::io::ErrorKind::InvalidData);
        assert!(queries.next().is_none());
    }
}
