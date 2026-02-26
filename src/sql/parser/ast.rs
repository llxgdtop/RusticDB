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
        // Starting from feat: Projection support, column names are also treated as expressions;
        // the second parameter indicates whether there is an alias.
        select: Vec<(Expression, Option<String>)>,
        from: FromItem,
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
        predicate: Option<Expression>, // join的on条件，如果是cross join则没有
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

// 表达式定义，目前不支持二元表达式，比如1+1
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Field(String), // 列名
    Consts(Consts), // 常量
    Operation(Operation), // 运算操作
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

#[derive(Debug, PartialEq, Clone)]
pub enum Operation {
    // 相等。比如说tbl1 join tbl2 on tbl1id = tbl2id
    // tbl1id就是tbl1的其中一个列，所以这里就用一个Box<Expression>
    // 当然也可以是其它的运算操作与参数
    Equal(Box<Expression>, Box<Expression>), 
}
