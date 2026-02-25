use std::collections::BTreeMap;

use crate::sql::types::DataType;

/// Abstract Syntax Tree (AST) node definitions for SQL statements
#[derive(Debug, PartialEq)]
pub enum Statement {
    /// CREATE TABLE statement
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    /// INSERT statement
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    /// SELECT statement
    Select {
        table_name: String,
        order_by: Vec<(String, OrderDirection)>,
    },
    /// UPDATE statement
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<(String, Expression)>,
    },
    /// DELETE statement
    Delete {
        table_name: String,
        where_clause: Option<(String, Expression)>,
    },
}

/// Sort direction (ascending or descending)
#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// Column definition for CREATE TABLE statements
#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

/// Expression definition (currently only constants)
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Consts(Consts),
}

/// Implements From trait to convert Consts into Expression
impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

/// Constant values in SQL expressions
#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}
