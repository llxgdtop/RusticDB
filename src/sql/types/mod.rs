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
        }
    }

    // 获取value的数据类型
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

/// A row is a vector of values
pub type Row = Vec<Value>;
