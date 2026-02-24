use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::{
        parser::ast::Expression, schema::Table, types::{Row, Value}
    },
    storage::{self, engine::Engine as StorageEngine, keycode::serialize_key},
};

use super::{Engine, Transaction};

/// Key-value store backed SQL engine
pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

/// Key-value transaction (wrapper around MVCC transaction)
pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(&self) -> Result<()> {
        self.txn.rollback()
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;

        // Validate row data types match table schema
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {}
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )))
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                }
                _ => {}
            }
        }

        // Get primary key as unique identifier for the row
        let pk = table.get_primary_key(&row)?;
        let id = Key::Row(table_name.clone(), pk.clone()).encode()?;
        // Check primary key uniqueness
        if self.txn.get(id.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "Duplicate data for primary key {} in table {}",
                pk, table_name
            )));
        }

        // Store the row data
        let value = bincode::serialize(&row)?;
        self.txn.set(id, value)?;


        Ok(())
    }

    /// Updates a row - if primary key changes, delete old data and insert new
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;
        // If primary key changed, delete the old data
        if *id != new_pk {
            let oldKey = Key::Row(table.name.clone(), id.clone()).encode()?;
            self.txn.delete(oldKey)?;
        }
        let key = Key::Row(table.name.clone(), new_pk.clone()).encode()?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;

        Ok(())
    }

    /// Deletes a row by primary key
    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()> {
        let key = Key::Row(table.name.clone(), id.clone()).encode()?;
        self.txn.delete(key)
    }

    fn scan_table(
        &self,
        table_name: String,
        filter: Option<(String, Expression)>,
    ) -> Result<Vec<Row>> {
        // Use prefix scan to find all rows in the table
        let prefix = KeyPrefix::Row(table_name.clone()).encode()?;
        let table = self.must_get_table(table_name)?;
        let results = self.txn.scan_prefix(prefix)?;

        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            // Apply filter if present
            if let Some((col, expr)) = &filter {
                let col_index = table.get_col_index(&col)?;
                if Value::from_expression(expr.clone()) == row[col_index] {
                    rows.push(row);
                }
            } else {
                // No filter, include all rows
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // Check if table already exists
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }

        // Validate table has at least one column
        table.validate()?;

        // Store table schema: key = table name, value = serialized table schema
        let key = Key::Table(table.name.clone()).encode()?;
        let value = bincode::serialize(&table)?; 
        self.txn.set(key, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name).encode()?;
        Ok(self
            .txn
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }
}

/// Key types for KV storage operations
#[derive(Debug, Serialize, Deserialize)]
enum Key {
    /// Table schema key (table name)
    Table(String),
    /// Row data key (table name + primary key value)
    Row(String, Value),
}

// Use custom serialization for prefix matching support with variable-length strings
impl Key {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

/// Key prefix types for prefix scanning
///
/// In bincode, enums are serialized as [variant_index][variant_data...].
/// Variant indices start from 0 in definition order.
#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

impl KeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        s.execute("select * from t1;")?;

        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b', 2);")?;
        s.execute("insert into t1 values(3, 'c', 3);")?;

        let v = s.execute("update t1 set b = 'aa' where a = 1;")?;
        let v = s.execute("update t1 set a = 33 where a = 3;")?;
        println!("{:?}", v);

        match s.execute("select * from t1;")? {
            crate::sql::executor::ResultSet::Scan { columns, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b', 2);")?;
        s.execute("insert into t1 values(3, 'c', 3);")?;

        s.execute("delete from t1 where a = 3;")?;
        s.execute("delete from t1 where a = 2;")?;

        match s.execute("select * from t1;")? {
            crate::sql::executor::ResultSet::Scan { columns, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
