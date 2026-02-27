use crate::{error::Result, sql::{engine::Transaction, executor::{agg::Aggregate, join::NestedLoopJoin, mutation::{Delete, Insert, Update}, query::{Limit, Offset, Order, Projection, Scan}, schema::CreateTable}, plan::Node, types::Row}};

mod agg;
mod schema;
mod mutation;
mod query;
mod join;

/// SQL executor trait
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

/// Builds an executor from a plan node
///
/// The `'static` bound is required for trait object usage in recursive executor building.
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
            Node::Delete { table_name, source } => Delete::new(table_name, Self::build(*source)),
            Node::Order { source, order_by } => Order::new(Self::build(*source), order_by),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Projection { source, exprs } => Projection::new(Self::build(*source), exprs),
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                outer,
            } => NestedLoopJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
            Node::Aggregate {
                source,
                exprs,
                group_by,
            } => Aggregate::new(Self::build(*source), exprs, group_by),
        }
    }
}

/// Execution result set
#[derive(Debug, PartialEq)]
pub enum ResultSet {
    CreateTable { table_name: String },
    Insert { count: usize },
    Scan { columns: Vec<String>, rows: Vec<Row> },
    Update { count: usize },
    Delete {
        count: usize,
    },
}
