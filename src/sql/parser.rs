// Copyright 2024 TiSQL Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::util::error::{Result, TiSqlError};

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
        SqlParser::parse_sql(&self.dialect, sql).map_err(|e| TiSqlError::Parse(e.to_string()))
    }

    /// Parse a single statement
    pub fn parse_one(&self, sql: &str) -> Result<SqlStatement> {
        let mut stmts = self.parse(sql)?;
        if stmts.is_empty() {
            return Err(TiSqlError::Parse("Empty SQL statement".into()));
        }
        if stmts.len() > 1 {
            return Err(TiSqlError::Parse(
                "Multiple statements not supported".into(),
            ));
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
