use crate::{error::Result, sql::{engine::Transaction, executor::{Executor, ResultSet}, schema::Table}};

/// CREATE TABLE executor
pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table_name = self.schema.name.clone();
        txn.create_table(self.schema)?;  // 转移所有权
        // 由于上方已经转移，返回schema.name会报错，所以单独将这个给clone一份用于返回
        Ok(ResultSet::CreateTable { table_name }) 
    }
}
