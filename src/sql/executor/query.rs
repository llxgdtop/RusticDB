use crate::error::Result;

use super::Executor;

/// Table scan executor (SELECT)
pub struct Scan {
    table_name: String,
}

impl Scan {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Self { table_name })
    }
}

impl Executor for Scan {
    fn execute(&self) -> Result<super::ResultSet> {
        todo!()
    }
}
