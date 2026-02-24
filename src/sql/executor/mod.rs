use crate::{error::Result, sql::{engine::Transaction, executor::{mutation::{Insert, Update}, query::Scan, schema::CreateTable}, plan::Node, types::Row}};

mod schema;
mod mutation;
mod query;

/// SQL executor trait
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

// 'static bound required for Update's recursive call with trait objects
impl<T: Transaction + 'static> dyn Executor<T> {
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name, filter } => Scan::new(table_name, filter),
            Node::Update {
                table_name,
                source,
                columns,
            } => Update::new(
                table_name,
                // Recursively build inner node (Scan node from planner.rs)
                Self::build(*source),
                columns),
        }
    }
}

/// Execution result set
#[derive(Debug)]
pub enum ResultSet {
    CreateTable { table_name: String },
    Insert { count: usize },
    Scan { columns: Vec<String>, rows: Vec<Row> },
    /// Number of rows updated
    Update { count: usize },
}
