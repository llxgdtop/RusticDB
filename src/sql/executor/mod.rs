use crate::{error::Result, sql::{engine::Transaction, executor::{agg::Aggregate, join::NestedLoopJoin, mutation::{Delete, Insert, Update}, query::{Filter, Limit, Offset, Order, Projection, Scan}, schema::CreateTable}, plan::Node, types::Row}};

mod agg;
mod schema;
mod mutation;
mod query;
mod join;

/// Executor trait for running execution plan nodes
///
/// Each executor consumes a plan node and produces a `ResultSet`.
/// Executors form a tree structure matching the plan tree, with each
/// node calling its children recursively during execution.
///
/// # Type Parameters
/// - `T`: Transaction type implementing [`Transaction`] trait
///
/// # Example
/// ```ignore
/// let executor = Executor::build(plan_node);
/// let result = executor.execute(&mut txn)?;
/// ```
pub trait Executor<T: Transaction> {
    /// Executes the plan node within the given transaction
    ///
    /// Takes `Box<Self>` to allow executors to consume themselves,
    /// avoiding additional allocation when building executor chains.
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
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
        }
    }
}

/// Execution result returned by SQL statements
#[derive(Debug, PartialEq)]
pub enum ResultSet {
    /// CREATE TABLE result
    CreateTable { table_name: String },
    /// INSERT result with number of rows inserted
    Insert { count: usize },
    /// SELECT/SCAN result with column names and row data
    Scan { columns: Vec<String>, rows: Vec<Row> },
    /// UPDATE result with number of rows modified
    Update { count: usize },
    /// DELETE result with number of rows deleted
    Delete { count: usize },
}
