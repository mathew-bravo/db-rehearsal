use chrono::{DateTime, Utc};

pub struct CapturedQuery {
    pub timestamp: DateTime<Utc>,
    pub connection_id: u64,
    pub command_type: CommandType,
}

pub enum CommandType {
    Query,
    Connect,
    Quit,
    Prepare,
    Execute,
    Other(String)
}

pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    DDL,
    Administrative,
    Other,
}