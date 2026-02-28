use crate::{error::{Error, Result}, sql::{parser::ast::Expression, types::Value}};

use super::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::Row};

mod kv;

/// SQL engine trait
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

/// SQL transaction trait (DDL and DML operations)
///
/// Can be backed by KV storage or distributed storage.
/// Each SQL engine can have its own transaction type (e.g., 2PL, OCC).
pub trait Transaction {
    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;
    /// Updates a row, id is the primary key
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()>;
    /// Deletes a row by primary key
    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()>;
    /// Scans table with optional filter
    fn scan_table(
        &self,
        table_name: String,
        filter: Option<Expression>,
    ) -> Result<Vec<Row>>;

    // DDL operations
    fn create_table(&mut self, table: Table) -> Result<()>;
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;
    /// Returns table info, returns error if table doesn't exist
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
            .ok_or(Error::Internal(format!(
                "table {} does not exist",
                table_name
            )))
    }
}

/// SQL session for executing statements
pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine + 'static> Session<E> {
    /// Executes a SQL statement
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                match Plan::build(stmt)?.execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}
