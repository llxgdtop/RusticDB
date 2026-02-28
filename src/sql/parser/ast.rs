use std::collections::BTreeMap;

use crate::{error::{Error, Result}, sql::types::{DataType, Value}};

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
        where_clause: Option<Expression>,
        /// GROUP BY expression (None means entire table is one group)
        group_by: Option<Expression>,
        having: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    /// UPDATE statement
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        /// WHERE clause filter condition
        /// Since the Expression enum includes Field(String) for column references,
        /// the where_clause can represent any expression (not just simple column comparisons)
        where_clause: Option<Expression>,
    },
    /// DELETE statement
    Delete {
        table_name: String,
        where_clause: Option<Expression>,
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
    /// Equality comparison (e.g., tbl1.id = tbl2.id)
    /// Uses Box<Expression> because the operand type (column, constant, etc.) is determined at runtime
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
}

/// Evaluates an expression against row data
///
/// Used for Operation evaluation:
/// 1. Get the value of a column in a row
/// 2. Compare two column values for equality, greater than, or less than
pub fn evaluate_expr(
    expr: &Expression,
    lcols: &Vec<String>, // Left table columns
    lrows: &Vec<Value>,  // Left table current row data
    rcols: &Vec<String>, // Right table columns
    rrows: &Vec<Value>,  // Right table current row data
) -> Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            let pos = match lcols.iter().position(|c| *c == *col_name) {
                Some(pos) => pos,
                None => {
                    return Err(Error::Internal(format!(
                        "column {} is not in table",
                        col_name
                    )))
                }
            };
            Ok(lrows[pos].clone())
        }
        // Constant expression: e.g., WHERE 1 = 1
        Expression::Consts(consts) => Ok(match consts {
            Consts::Null => Value::Null,
            Consts::Boolean(b) => Value::Boolean(*b),
            Consts::Integer(i) => Value::Integer(*i),
            Consts::Float(f) => Value::Float(*f),
            Consts::String(s) => Value::String(s.clone()),
        }),
        // Operation: recursively evaluate left and right expressions, then compare
        Expression::Operation(operation) => match operation {
            Operation::Equal(lexpr, rexpr) => {
                // Recursively evaluate left expression
                let lv = evaluate_expr(lexpr, lcols, lrows, rcols, rrows)?;
                // Recursively evaluate right expression (swap params since Field uses lcols)
                let rv = evaluate_expr(rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    // Return true/false for equality comparison
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare expression {} and {}",
                            l, r
                        )))
                    }
                })
            }
            Operation::GreaterThan(lexpr, rexpr) => {
                let lv = evaluate_expr(lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 > r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l > r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l > r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare expression {} and {}",
                            l, r
                        )))
                    }
                })
            }
            Operation::LessThan(lexpr, rexpr) => {
                let lv = evaluate_expr(lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean((l as f64) < r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l < r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l < r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare expression {} and {}",
                            l, r
                        )))
                    }
                })
            }
        },
        _ => return Err(Error::Internal("unexpected expression".into())),
    }
}