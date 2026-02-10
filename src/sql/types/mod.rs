use crate::sql::parser::ast::{Consts, Expression};

/// Supported SQL data types
#[derive(Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

/// Runtime value type for expressions
#[derive(Debug, PartialEq)]
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
        }
    }
}

/// A row is a vector of values
pub type Row = Vec<Value>;
