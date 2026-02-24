//! SQL processing module
//!
//! This module provides:
//! - `parser`: SQL lexer and parser
//! - `types`: SQL data types
//! - `schema`: Table and column schema definitions
//! - `plan`: Execution plan generation
//! - `executor`: Query and mutation execution
//! - `engine`: Storage engine abstraction

pub mod parser;
pub mod types;
pub mod schema;
pub mod plan;
pub mod executor;
pub mod engine;