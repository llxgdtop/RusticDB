use crate::{error::Result, sql::{executor::{mutation::Insert, query::Scan, schema::CreateTable}, plan::Node, types::Row}};

mod schema;
mod mutation;
mod query;

/// SQL executor trait
pub trait Executor {
    fn execute(&self) -> Result<ResultSet>;
}

impl dyn Executor {
    /// Builds an executor from an execution plan node
    pub fn build(node: Node) -> Box<dyn Executor> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}

/// Execution result set
pub enum ResultSet {
    CreateTable { table_name: String },
    Insert { count: usize },
    Scan { columns: Vec<String>, rows: Vec<Row> },
}
