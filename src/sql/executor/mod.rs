use crate::{error::Result, sql::{engine::Transaction, executor::{mutation::{Insert, Update}, query::Scan, schema::CreateTable}, plan::Node, types::Row}};

mod schema;
mod mutation;
mod query;

/// SQL executor trait
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

// 要加上static是因为在Update的时候递归调用可能T的生命周期不够长
// 即编译器不知道T的生命周期是否大于等于source的生命周期，所以要转成静态生命周期
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
                // 递归调用这个build方法，继续执行里面这层节点先
                // 其实这是一个Scan节点的执行，因为在planner.rs中传入的就是Scan
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
    Update {
        count: usize,
    }, // 更新数量
}
