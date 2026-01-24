use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::error::{Result, TiSqlError};

/// SQL Parser wrapper around sqlparser-rs
pub struct Parser {
    dialect: MySqlDialect,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            dialect: MySqlDialect {},
        }
    }

    /// Parse SQL string into AST statements
    pub fn parse(&self, sql: &str) -> Result<Vec<SqlStatement>> {
        SqlParser::parse_sql(&self.dialect, sql)
            .map_err(|e| TiSqlError::Parse(e.to_string()))
    }

    /// Parse a single statement
    pub fn parse_one(&self, sql: &str) -> Result<SqlStatement> {
        let mut stmts = self.parse(sql)?;
        if stmts.is_empty() {
            return Err(TiSqlError::Parse("Empty SQL statement".into()));
        }
        if stmts.len() > 1 {
            return Err(TiSqlError::Parse("Multiple statements not supported".into()));
        }
        Ok(stmts.remove(0))
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let parser = Parser::new();
        let stmt = parser.parse_one("SELECT 1 + 1").unwrap();
        assert!(matches!(stmt, SqlStatement::Query(_)));
    }

    #[test]
    fn test_parse_create_table() {
        let parser = Parser::new();
        let stmt = parser
            .parse_one("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))")
            .unwrap();
        assert!(matches!(stmt, SqlStatement::CreateTable { .. }));
    }

    #[test]
    fn test_parse_insert() {
        let parser = Parser::new();
        let stmt = parser
            .parse_one("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        assert!(matches!(stmt, SqlStatement::Insert { .. }));
    }
}
