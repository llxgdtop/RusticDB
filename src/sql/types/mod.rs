use std::{cmp::Ordering, fmt::Display, hash::Hash};

use serde::{Deserialize, Serialize};
use crate::sql::parser::ast::{Consts, Expression};

/// Supported SQL data types
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

/// Runtime value type for expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    /// Creates a Value from an AST expression
    pub fn from_expression(expr: Expression) -> Self {
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
            _ => unreachable!(), // Column expressions are handled separately
        }
    }

    /// Returns the data type of the value, or None if it's Null
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "{}", "NULL"),
            Value::Boolean(b) if *b => write!(f, "{}", "TRUE"),
            Value::Boolean(_) => write!(f, "{}", "FALSE"),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
        }
    }
}

/// Implements partial ordering for Value comparison (used by ORDER BY)
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (_, _) => None,
        }
    }
}

/// Implements Hash for Value to enable use as HashMap key (required for GROUP BY)
///
/// Uses a type discriminator byte (write_u8) to distinguish between variants,
/// then delegates to the underlying type's hash implementation.
impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => state.write_u8(0),
            Value::Boolean(v) => {
                state.write_u8(1);
                v.hash(state);
            }
            Value::Integer(v) => {
                state.write_u8(2);
                v.hash(state);
            }
            Value::Float(v) => {
                state.write_u8(3);
                v.to_be_bytes().hash(state);
            }
            Value::String(v) => {
                state.write_u8(2);
                v.hash(state);
            }
        }
    }
}

impl Eq for Value {}

/// A row is a vector of values
pub type Row = Vec<Value>;
