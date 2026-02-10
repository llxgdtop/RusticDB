use crate::{error::Result, sql::{executor::Executor, schema::Table}};

/// CREATE TABLE executor
pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl Executor for CreateTable {
    fn execute(&self) -> Result<super::ResultSet> {
        todo!()
    }
}
