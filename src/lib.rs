//! RustDB - A simple relational database implementation in Rust
//!
//! This crate provides a minimal SQL database with:
//! - SQL parsing (lexer, parser, AST)
//! - Query planning and execution
//! - MVCC-based transaction support
//! - Pluggable storage engines

pub mod error;
pub mod sql;
pub mod storage;
