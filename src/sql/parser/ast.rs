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
        /// Column expressions with optional aliases (e.g., Count(*) as cnt)
        select: Vec<(Expression, Option<String>)>,
        from: FromItem,
        /// GROUP BY expression (None means entire table is one group)
        group_by: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
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

/// FROM clause item - represents a table or join expression
#[derive(Debug, PartialEq)]
pub enum FromItem {
    /// Single table reference
    Table {
        name: String,
    },

    /// Join expression (two tables joined together)
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        /// Join ON condition (None for CROSS JOIN)
        predicate: Option<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
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

/// Expression types (column refs, constants, operations, aggregate functions)
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    /// Column reference
    Field(String),
    /// Constant value
    Consts(Consts),
    /// Binary operation (e.g., equality comparison)
    Operation(Operation),
    /// Aggregate function: Function(name, column) e.g., Function("count", "id")
    Function(String, String),
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

/// Binary operations
#[derive(Debug, PartialEq, Clone)]
pub enum Operation {
    /// Equality comparison (e.g., t1.id = t2.id in JOIN ON clause)
    Equal(Box<Expression>, Box<Expression>),
}
