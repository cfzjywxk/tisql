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

pub type IdentifierRef<'a> = &'a str;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionRef<'a> {
    All,
    Columns(Vec<&'a str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointGetShortQuery<'a> {
    pub table: IdentifierRef<'a>,
    pub projection: ProjectionRef<'a>,
    pub pk_column: IdentifierRef<'a>,
    // String literals keep surrounding single quotes.
    pub pk_literal: &'a str,
}

pub fn starts_with_select_ascii_case_insensitive(sql: &str) -> bool {
    let mut p = Cursor::new(sql);
    p.skip_ws();
    p.consume_keyword("select")
}

pub fn try_parse_point_get_short(sql: &str) -> Option<PointGetShortQuery<'_>> {
    let mut p = Cursor::new(sql);
    p.skip_ws();
    p.expect_keyword("select")?;
    let projection = p.parse_projection()?;
    p.expect_keyword("from")?;
    let table = p.parse_identifier()?;
    p.expect_keyword("where")?;
    let pk_column = p.parse_identifier()?;
    p.skip_ws();
    p.expect_byte(b'=')?;
    p.skip_ws();
    let pk_literal = p.parse_literal()?;
    p.skip_ws();
    p.consume_byte(b';');
    p.skip_ws();
    if !p.is_eof() {
        return None;
    }

    Some(PointGetShortQuery {
        table,
        projection,
        pk_column,
        pk_literal,
    })
}

struct Cursor<'a> {
    s: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            s,
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn consume_byte(&mut self, target: u8) -> bool {
        if self.peek() == Some(target) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_byte(&mut self, target: u8) -> Option<()> {
        self.consume_byte(target).then_some(())
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        let kw = keyword.as_bytes();
        if self.pos + kw.len() > self.bytes.len() {
            return false;
        }
        let end = self.pos + kw.len();
        if !self.s[self.pos..end].eq_ignore_ascii_case(keyword) {
            return false;
        }
        if let Some(next) = self.bytes.get(end).copied() {
            if is_identifier_char(next) {
                return false;
            }
        }
        self.pos = end;
        true
    }

    fn expect_keyword(&mut self, keyword: &str) -> Option<()> {
        self.skip_ws();
        self.consume_keyword(keyword).then_some(())
    }

    fn parse_projection(&mut self) -> Option<ProjectionRef<'a>> {
        self.skip_ws();
        if self.consume_byte(b'*') {
            return Some(ProjectionRef::All);
        }

        let mut cols = Vec::new();
        loop {
            self.skip_ws();
            cols.push(self.parse_identifier()?);
            self.skip_ws();
            if !self.consume_byte(b',') {
                break;
            }
        }
        if cols.is_empty() {
            return None;
        }
        Some(ProjectionRef::Columns(cols))
    }

    fn parse_identifier(&mut self) -> Option<&'a str> {
        self.skip_ws();
        if self.peek() == Some(b'`') {
            self.pos += 1;
            let start = self.pos;
            while let Some(b) = self.peek() {
                if b == b'`' {
                    let end = self.pos;
                    self.pos += 1;
                    return Some(&self.s[start..end]);
                }
                self.pos += 1;
            }
            return None;
        }

        let start = self.pos;
        let first = self.peek()?;
        if !is_identifier_start(first) {
            return None;
        }
        self.pos += 1;
        while let Some(b) = self.peek() {
            if is_identifier_char(b) {
                self.pos += 1;
            } else {
                break;
            }
        }
        Some(&self.s[start..self.pos])
    }

    fn parse_literal(&mut self) -> Option<&'a str> {
        self.skip_ws();
        match self.peek()? {
            b'\'' => self.parse_single_quoted(),
            b'+' | b'-' | b'0'..=b'9' => self.parse_signed_integer(),
            _ => None,
        }
    }

    fn parse_single_quoted(&mut self) -> Option<&'a str> {
        let start = self.pos;
        self.pos += 1; // opening quote

        while let Some(b) = self.peek() {
            if b == b'\'' {
                // SQL string escape: '' -> one quote
                if self.bytes.get(self.pos + 1).copied() == Some(b'\'') {
                    self.pos += 2;
                    continue;
                }
                self.pos += 1; // closing quote
                return Some(&self.s[start..self.pos]);
            }
            self.pos += 1;
        }
        None
    }

    fn parse_signed_integer(&mut self) -> Option<&'a str> {
        let start = self.pos;
        if matches!(self.peek(), Some(b'+') | Some(b'-')) {
            self.pos += 1;
        }
        let digit_start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        if self.pos == digit_start {
            return None;
        }
        Some(&self.s[start..self.pos])
    }
}

fn is_identifier_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_identifier_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_star_projection() {
        let q = try_parse_point_get_short("SELECT * FROM usertable WHERE YCSB_KEY = 'user123';")
            .unwrap();
        assert_eq!(q.table, "usertable");
        assert_eq!(q.pk_column, "YCSB_KEY");
        assert_eq!(q.pk_literal, "'user123'");
        assert_eq!(q.projection, ProjectionRef::All);
    }

    #[test]
    fn parse_column_projection_with_backticks() {
        let q = try_parse_point_get_short(
            "select `f1`, `f2` from `usertable` where `YCSB_KEY`='user42'",
        )
        .unwrap();
        assert_eq!(q.table, "usertable");
        assert_eq!(q.pk_column, "YCSB_KEY");
        assert_eq!(q.pk_literal, "'user42'");
        assert_eq!(q.projection, ProjectionRef::Columns(vec!["f1", "f2"]));
    }

    #[test]
    fn parse_signed_numeric_literal() {
        let q = try_parse_point_get_short("SELECT v FROM t WHERE id = -001").unwrap();
        assert_eq!(q.pk_literal, "-001");
    }

    #[test]
    fn parse_single_quoted_literal_with_escape() {
        let q = try_parse_point_get_short("SELECT * FROM t WHERE id = 'ab''cd'").unwrap();
        assert_eq!(q.pk_literal, "'ab''cd'");
    }

    #[test]
    fn reject_non_point_get_shapes() {
        assert!(try_parse_point_get_short("SELECT * FROM t WHERE id = 'k' AND v = 1").is_none());
        assert!(try_parse_point_get_short("SELECT * FROM t WHERE id = 'k' ORDER BY id").is_none());
        assert!(try_parse_point_get_short("SELECT * FROM t WHERE id = 'k' LIMIT 1").is_none());
        assert!(try_parse_point_get_short("SELECT now() FROM t WHERE id = 'k'").is_none());
        assert!(try_parse_point_get_short("SELECT * FROM t").is_none());
    }

    #[test]
    fn starts_with_select_precheck() {
        assert!(starts_with_select_ascii_case_insensitive(" SELECT 1"));
        assert!(starts_with_select_ascii_case_insensitive("sElEcT 1"));
        assert!(!starts_with_select_ascii_case_insensitive(
            "SET autocommit = 1"
        ));
        assert!(!starts_with_select_ascii_case_insensitive("selective"));
    }
}
