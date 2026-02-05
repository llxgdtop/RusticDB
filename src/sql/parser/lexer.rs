//! SQL Lexer - Tokenizes SQL input text into a stream of tokens
//!
//! This module provides the lexical analysis (tokenization) phase of the SQL parser.
//! It breaks down raw SQL text into meaningful tokens that can be consumed by the parser.

use std::{iter::Peekable,str::Chars};

use crate::error::{Result,Error};

/// Represents a single lexical token in the SQL input
///
/// Tokens are the smallest meaningful units produced by the lexer,
/// such as keywords, identifiers, literals, and operators.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// SQL reserved keyword (e.g., SELECT, FROM, WHERE)
    Keyword(Keyword),
    /// Identifier such as table name or column name
    Ident(String),
    /// String literal enclosed in single quotes
    String(String),
    /// Numeric literal (integer or floating-point)
    Number(String),
    /// Left parenthesis `(`
    OpenParen,
    /// Right parenthesis `)`
    CloseParen,
    /// Comma `,`
    Comma,
    /// Semicolon `;`
    Semicolon,
    /// Asterisk `*` (typically used for SELECT *)
    Asterisk,
    /// Plus operator `+`
    Plus,
    /// Minus operator `-`
    Minus,
    /// Forward slash `/` (division operator)
    Slash,
}

/// SQL reserved keywords
///
/// These are special words in SQL that have predefined meanings.
/// Keywords are case-insensitive during parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    // DDL keywords
    Create,
    Table,
    // Data type keywords
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    // DML keywords
    Select,
    From,
    Insert,
    Into,
    Values,
    // Literal keywords
    True,
    False,
    Default,
    Not,
    Null,
    // Constraint keywords
    Primary,
    Key,
}

impl Keyword {
    /// Attempts to parse a string as a keyword
    ///
    /// # Arguments
    /// * `ident` - The identifier string to parse (case-insensitive)
    ///
    /// # Returns
    /// * `Some(Keyword)` if the identifier matches a known keyword
    /// * `None` if the identifier is not a keyword (should be treated as a regular identifier)
    pub fn from_str(ident: &str) -> Option<Keyword> {
        Some(match ident.to_uppercase().as_ref() {
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "BOOL" => Keyword::Bool,
            "STRING" => Keyword::String,
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            _ => return None,
        })
    }
}

/// SQL lexical analyzer (lexer/tokenizer)
///
/// The lexer breaks down SQL input text into a sequence of tokens.
/// It uses a peekable iterator to examine characters without consuming them,
/// enabling look-ahead for proper token recognition.
///
/// # Lifetime
/// * `'a` - The lifetime of the SQL text being tokenized
pub struct Lexer<'a> {
    /// Peekable character iterator for look-ahead capability
    iter: Peekable<Chars<'a>>,
}

/// Implements Iterator trait to enable token streaming
///
/// This allows the lexer to be used with Rust's iterator adapters,
/// such as `collect()`, `map()`, `filter()`, etc.
impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self
                .iter
                .peek()
                .map(|c| Err(Error::Parse(format!("[Lexer] Unexpeted character {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    /// Creates a new lexer for the given SQL text
    ///
    /// # Arguments
    /// * `sql_text` - The SQL input string to tokenize
    pub fn new(sql_text: &'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    /// Consumes and returns the next character if it satisfies the predicate
    ///
    /// # Arguments
    /// * `predicate` - A closure that returns `true` if the character should be consumed
    ///
    /// # Returns
    /// * `Some(char)` - The consumed character
    /// * `None` - If the predicate fails or no character is available
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char>{
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    /// Consumes consecutive characters while they satisfy the predicate
    ///
    /// # Arguments
    /// * `predicate` - A closure that returns `true` while characters should be consumed
    ///
    /// # Returns
    /// * `Some(String)` - The collected characters
    /// * `None` - If no characters were collected
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String>{
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }

        Some(value).filter(|v| !v.is_empty())
    }

    /// Peeks at the next character and returns a token if the predicate produces one
    ///
    /// Unlike `next_if`, this takes a predicate that returns a `Token` instead of a bool.
    /// This is useful for single-character tokens like operators and punctuation.
    ///
    /// # Arguments
    /// * `predicate` - A closure that maps a character to an optional Token
    ///
    /// # Returns
    /// * `Some(Token)` - If the character maps to a known token
    /// * `None` - If the character doesn't map to any token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token>{
        let token = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(token)
    }

    /// Removes all whitespace characters from the input stream
    fn erase_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    /// Scans the input and returns the next token
    ///
    /// This is the main dispatch method that determines what type of token
    /// comes next based on the first character.
    ///
    /// # Returns
    /// * `Ok(Some(Token))` - A valid token was found
    /// * `Ok(None)` - End of input reached
    /// * `Err(Error)` - A lexical error occurred
    fn scan(&mut self) -> Result<Option<Token>>{
        self.erase_whitespace();
        match self.iter.peek() {
            Some('\'') => self.scan_string(),
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }
    }

    /// Scans a string literal (text enclosed in single quotes)
    ///
    /// String literals start with a single quote (`'`) and continue until
    /// the closing single quote. The quotes themselves are not included
    /// in the returned token value.
    ///
    /// # Returns
    /// * `Ok(Some(Token::String(...)))` - A complete string literal
    /// * `Err(Error)` - If the string is not properly closed
    fn scan_string(&mut self) -> Result<Option<Token>>{
        self.iter.next(); // Consume opening quote
        let mut val = String::new();

        loop {
            match self.iter.next(){
                Some('\'') => break,
                Some(c) => val.push(c),
                None => return Err(Error::Parse(format!("[Lexer] Unexpected end of string"))),
            }
        }
        Ok(Some(Token::String(val)))
    }

    /// Scans a numeric literal (integer or floating-point)
    ///
    /// Numbers consist of one or more digits, optionally followed by
    /// a decimal point and more digits (e.g., `123`, `45.67`).
    ///
    /// # Returns
    /// * `Some(Token::Number(...))` - A valid numeric literal
    /// * `None` - If no digits are found
    fn scan_number(&mut self) -> Option<Token> {
        let mut val = self.next_while(|c| c.is_ascii_digit())?;
        // Handle decimal point for floating-point numbers
        if let Some(sep) = self.next_if(|c| c == '.') {
            val.push(sep);
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                val.push(c);
            }
        }
        Some(Token::Number(val))
    }

    /// Scans an identifier or keyword
    ///
    /// Identifiers start with a letter and may contain letters, digits,
    /// and underscores (e.g., `table_name`, `col1`).
    ///
    /// After scanning, the identifier is checked against the keyword list.
    /// If it matches a keyword, a `Token::Keyword` is returned; otherwise,
    /// a `Token::Ident` is returned with the identifier converted to lowercase.
    ///
    /// # Returns
    /// * `Some(Token::Keyword(...))` - If the identifier is a reserved keyword
    /// * `Some(Token::Ident(...))` - If it's a regular identifier
    /// * `None` - If no valid identifier is found
    fn scan_ident(&mut self) -> Option<Token> {
        let mut val = self.next_if(|c| c.is_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
           val.push(c);
        };
        // Check if identifier is a keyword; if not, return as regular identifier
        Some(Keyword::from_str(&val).map_or(Token::Ident(val.to_lowercase()), Token::Keyword))
    }

    /// Scans a single-character symbol token
    ///
    /// Symbols include operators and punctuation marks such as `+`, `-`, `*`,
    /// `/`, `(`, `)`, `,`, and `;`.
    ///
    /// # Returns
    /// * `Some(Token)` - If the character is a recognized symbol
    /// * `None` - If the character is not a known symbol
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::Lexer;
    use crate::{
        error::Result,
        sql::parser::lexer::{Keyword, Token},
    };

    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let tokens1 = Lexer::new(
            "CREATE table tbl
                (
                    id1 int primary key,
                    id2 integer
                );
                ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon
            ]
        );

        let tokens2 = Lexer::new(
            "CREATE table tbl
                        (
                            id1 int primary key,
                            id2 integer,
                            c1 bool null,
                            c2 boolean not null,
                            c3 float null,
                            c4 double,
                            c5 string,
                            c6 text,
                            c7 varchar default 'foo',
                            c8 int default 100,
                            c9 integer
                        );
                        ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert!(tokens2.len() > 0);

        Ok(())
    }

    #[test]
    fn test_lexer_insert_into() -> Result<()> {
        let tokens1 = Lexer::new("insert into tbl values (1, 2, '3', true, false, 4.55);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        let tokens2 = Lexer::new("INSERT INTO       tbl (id, name, age) values (100, 'db', 10);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id".to_string()),
                Token::Comma,
                Token::Ident("name".to_string()),
                Token::Comma,
                Token::Ident("age".to_string()),
                Token::CloseParen,
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_select() -> Result<()> {
        let tokens1 = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("tbl".to_string()),
                Token::Semicolon,
            ]
        );
        Ok(())
    }
}
