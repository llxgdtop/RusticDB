use std::collections::BTreeMap;
use std::iter::Peekable;
use ast::Column;
use crate::sql::parser::ast::Expression;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::error::{Result, Error};
use super::types::DataType;

pub mod ast;
mod lexer;

/// SQL Parser - Converts tokens into Abstract Syntax Tree (AST)
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    /// Creates a new parser for the given SQL input
    pub fn new(input: &'a str) -> Self {
        Parser { lexer: Lexer::new(input).peekable() }
    }

    /// Parses the input SQL statement into an AST
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;
        self.next_expect(Token::Semicolon)?;
        // No tokens allowed after semicolon
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    /// Parses a statement based on the first token
    fn parse_statement(&mut self) -> Result<ast::Statement> {
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }

    /// Parses DDL statements (e.g., CREATE TABLE)
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
        }
    }

    /// Parses CREATE TABLE statement
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        let table_name = self.next_ident()?;
        self.next_expect(Token::OpenParen)?;

        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable { name: table_name, columns })
    }

    /// Parses column definition in CREATE TABLE
    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => DataType::Boolean,
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String) | Token::Keyword(Keyword::Text) | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            nullable: None,
            default: None,
            primary_key: false,
        };

        // Parse column constraints (NULL, NOT NULL, DEFAULT)
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Primary => {
                    self.next_expect(Token::Keyword(Keyword::Key))?;
                    column.primary_key = true;
                }
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}", k))),
            }
        }

        Ok(column)
    }

    /// Parses SELECT statement (currently only supports SELECT * FROM table)
    fn parse_select(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Select))?;
        self.next_expect(Token::Asterisk)?;
        self.next_expect(Token::Keyword(Keyword::From))?;

        let table_name = self.next_ident()?;
        Ok(ast::Statement::Select { table_name })
    }

    /// Parses INSERT statement
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;

        let table_name = self.next_ident()?;

        // Check if specific columns are specified
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_ident()?.to_string());
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            Some(cols)
        } else {
            None
        };

        self.next_expect(Token::Keyword(Keyword::Values))?;
        // Parse multiple value rows: INSERT INTO tbl VALUES (1,2),(3,4);
        let mut values = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut expr = Vec::new();
            loop {
                expr.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            values.push(expr);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(ast::Statement::Insert {
            table_name,
            columns,
            values,
        })
    }

    // 解析 Update 语句
    fn parse_update(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Update))?;
        // 表名
        let table_name = self.next_ident()?;
        self.next_expect(Token::Keyword(Keyword::Set))?;

        // 要更新的列信息
        let mut columns = BTreeMap::new();
        loop {
            let col = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            let value = self.parse_expression()?;
            // 如果重复更新（比如一个update语句里面又有a=1，又有a=2）就是错的
            if columns.contains_key(&col) {
                return Err(Error::Parse(format!(
                    "[parser] Duplicate column {} for update",
                    col
                )));
            }
            columns.insert(col, value);
            // 如果没有逗号，列解析完成，跳出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(ast::Statement::Update {
            table_name,
            columns,
            where_clause: self.parse_where_clause()?, // 解析where条件
        })
    }

    /// Parses an expression (currently only constants)
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                // Lexer scans both 123 and 123.45 as Token::Number(String)
                // Need to distinguish between integer and float here
                if n.chars().all(|c| c.is_ascii_digit()) {
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    ast::Consts::Float(n.parse()?).into()
                }
            }
            Token::String(s) => ast::Consts::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            t => {
                return Err(Error::Parse(format!(
                    "[Parser] Unexpected expression token {}",
                    t
                )))
            }
        })
    }

    // 解析where条件，column_name = expr
    fn parse_where_clause(&mut self) -> Result<Option<(String, Expression)>> {
        if self.next_if_token(Token::Keyword(Keyword::Where)).is_none() {
            return Ok(None) // 说明不限制条件
        }
        let col = self.next_ident()?;
        self.next_expect(Token::Equal)?;
        let val = self.parse_expression()?;
        Ok(Some((col, val)))
    }

    /// Peeks at the next token
    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    /// Consumes and returns the next token
    fn next(&mut self) -> Result<Token> {
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse(format!("[Parser] Unexpected end of input"))))
    }

    /// Expects and consumes an identifier
    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!(
                "[Parser] Expected ident, got token {}",
                token
            ))),
        }
    }

    /// Expects a specific token, returns error if different
    fn next_expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!(
                "[Parser] Expected token {}, got {}",
                expect, token
            )));
        }
        Ok(())
    }

    /// Consumes next token if it satisfies the predicate
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    /// Consumes next token if it's a keyword
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    /// Consumes next token if it matches the given token
    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}

/*
CREATE TABLE product (
    is_available BOOLEAN NOT NULL DEFAULT FALSE,
    stock_quantity INTEGER,
    product_name VARCHAR(100) NOT NULL DEFAULT '',
    price FLOAT,
    category_id INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    is_featured BOOLEAN,
    weight FLOAT NOT NULL DEFAULT 0.0
);

INSERT INTO product (
    is_available, stock_quantity, product_name, price, category_id, description, is_featured, weight
) VALUES (
    TRUE, 50, 'Wireless Mouse', 29.99, 3, 'Ergonomic wireless mouse with 2.4G receiver', TRUE, 0.15
);

INSERT INTO product (product_name, price, stock_quantity)
VALUES ('USB Cable', 9.99, 200);
*/

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::parser::ast};

    use super::Parser;

    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt1 = Parser::new(sql1).parse()?;

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(stmt1, stmt2);

        let sql3 = "
            create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        )
        ";

        let stmt3 = Parser::new(sql3).parse();
        assert!(stmt3.is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1,
            ast::Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into(),
                ]],
            }
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(
            stmt2,
            ast::Statement::Insert {
                table_name: "tbl2".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into(),
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into(),
                    ],
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let sql = "select * from tbl1;";
        let stmt = Parser::new(sql).parse()?;
        println!("{:?}", stmt);
        Ok(())
    }
}
