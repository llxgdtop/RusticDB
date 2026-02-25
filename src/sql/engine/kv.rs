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

        let pk = table.get_primary_key(&row)?;
        let id = Key::Row(table_name.clone(), pk.clone()).encode()?;
        if self.txn.get(id.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "Duplicate data for primary key {} in table {}",
                pk, table_name
            )));
        }

        let value = bincode::serialize(&row)?;
        self.txn.set(id, value)?;

        Ok(())
    }

    /// Updates a row - if primary key changes, delete old data and insert new
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;
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
        let prefix = KeyPrefix::Row(table_name.clone()).encode()?;
        let table = self.must_get_table(table_name)?;
        let results = self.txn.scan_prefix(prefix)?;

        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            if let Some((col, expr)) = &filter {
                let col_index = table.get_col_index(&col)?;
                if Value::from_expression(expr.clone()) == row[col_index] {
                    rows.push(row);
                }
            } else {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }

        table.validate()?;

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

    use super::KVEngine;
    use crate::storage::engine::Engine as StorageEngine;
    use crate::{
        error::Result,
        sql::{
            engine::{Engine, Session},
            executor::ResultSet,
            types::{Row, Value},
        },
        storage::memory::MemoryEngine,
    };

    fn setup_table<E: StorageEngine + 'static>(s: &mut Session<KVEngine<E>>) -> Result<()> {
        s.execute(
            "create table t1 (
                     a int primary key,
                     b text default 'vv',
                     c integer default 100
                 );",
        )?;

        s.execute(
            "create table t2 (
                     a int primary key,
                     b integer default 100,
                     c float default 1.1,
                     d bool default false,
                     e boolean default true,
                     f text default 'v1',
                     g string default 'v2',
                     h varchar default 'v3'
                 );",
        )?;

        s.execute(
            "create table t3 (
                     a int primary key,
                     b int default 12 null,
                     c integer default NULL,
                     d float not NULL
                 );",
        )?;

        s.execute(
            "create table t4 (
                     a bool primary key,
                     b int default 12,
                     d boolean default true
                 );",
        )?;
        Ok(())
    }

    fn scan_table_and_compare<E: StorageEngine + 'static>(
        s: &mut Session<KVEngine<E>>,
        table_name: &str,
        expect: Vec<Row>,
    ) -> Result<()> {
        match s.execute(&format!("select * from {};", table_name))? {
            ResultSet::Scan { columns: _, rows } => {
                assert_eq!(rows, expect);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn scan_table_and_print<E: StorageEngine + 'static>(
        s: &mut Session<KVEngine<E>>,
        table_name: &str,
    ) -> Result<()> {
        match s.execute(&format!("select * from {};", table_name))? {
            ResultSet::Scan { columns: _, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;
        Ok(())
    }

    #[test]
    fn test_insert() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        // t1
        s.execute("insert into t1 (a) values (1);")?;
        s.execute("insert into t1 values (2, 'a', 2);")?;
        s.execute("insert into t1(b,a) values ('b', 3);")?;

        scan_table_and_compare(
            &mut s,
            "t1",
            vec![
                vec![
                    Value::Integer(1),
                    Value::String("vv".to_string()),
                    Value::Integer(100),
                ],
                vec![
                    Value::Integer(2),
                    Value::String("a".to_string()),
                    Value::Integer(2),
                ],
                vec![
                    Value::Integer(3),
                    Value::String("b".to_string()),
                    Value::Integer(100),
                ],
            ],
        )?;

        // t2
        s.execute("insert into t2 (a) values (1);")?;
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![vec![
                Value::Integer(1),
                Value::Integer(100),
                Value::Float(1.1),
                Value::Boolean(false),
                Value::Boolean(true),
                Value::String("v1".to_string()),
                Value::String("v2".to_string()),
                Value::String("v3".to_string()),
            ]],
        )?;

        // t3
        s.execute("insert into t3 (a, d) values (1, 1.1);")?;
        scan_table_and_compare(
            &mut s,
            "t3",
            vec![vec![
                Value::Integer(1),
                Value::Integer(12),
                Value::Null,
                Value::Float(1.1),
            ]],
        )?;

        // t4
        s.execute("insert into t4 (a) values (true);")?;
        scan_table_and_compare(
            &mut s,
            "t4",
            vec![vec![
                Value::Boolean(true),
                Value::Integer(12),
                Value::Boolean(true),
            ]],
        )?;

        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t2 values (1, 1, 1.1, true, true, 'v1', 'v2', 'v3');")?;
        s.execute("insert into t2 values (2, 2, 2.2, false, false, 'v4', 'v5', 'v6');")?;
        s.execute("insert into t2 values (3, 3, 3.3, true, false, 'v7', 'v8', 'v9');")?;
        s.execute("insert into t2 values (4, 4, 4.4, false, true, 'v10', 'v11', 'v12');")?;

        let res = s.execute("update t2 set b = 100 where a = 1;")?;
        assert_eq!(res, ResultSet::Update { count: 1 });
        let res = s.execute("update t2 set d = false where d = true;")?;
        assert_eq!(res, ResultSet::Update { count: 2 });

        scan_table_and_compare(
            &mut s,
            "t2",
            vec![
                vec![
                    Value::Integer(1),
                    Value::Integer(100),
                    Value::Float(1.1),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v1".to_string()),
                    Value::String("v2".to_string()),
                    Value::String("v3".to_string()),
                ],
                vec![
                    Value::Integer(2),
                    Value::Integer(2),
                    Value::Float(2.2),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v4".to_string()),
                    Value::String("v5".to_string()),
                    Value::String("v6".to_string()),
                ],
                vec![
                    Value::Integer(3),
                    Value::Integer(3),
                    Value::Float(3.3),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v7".to_string()),
                    Value::String("v8".to_string()),
                    Value::String("v9".to_string()),
                ],
                vec![
                    Value::Integer(4),
                    Value::Integer(4),
                    Value::Float(4.4),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v10".to_string()),
                    Value::String("v11".to_string()),
                    Value::String("v12".to_string()),
                ],
            ],
        )?;

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t2 values (1, 1, 1.1, true, true, 'v1', 'v2', 'v3');")?;
        s.execute("insert into t2 values (2, 2, 2.2, false, false, 'v4', 'v5', 'v6');")?;
        s.execute("insert into t2 values (3, 3, 3.3, true, false, 'v7', 'v8', 'v9');")?;
        s.execute("insert into t2 values (4, 4, 4.4, false, true, 'v10', 'v11', 'v12');")?;

        let res = s.execute("delete from t2 where a = 1;")?;
        assert_eq!(res, ResultSet::Delete { count: 1 });
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![
                vec![
                    Value::Integer(2),
                    Value::Integer(2),
                    Value::Float(2.2),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v4".to_string()),
                    Value::String("v5".to_string()),
                    Value::String("v6".to_string()),
                ],
                vec![
                    Value::Integer(3),
                    Value::Integer(3),
                    Value::Float(3.3),
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::String("v7".to_string()),
                    Value::String("v8".to_string()),
                    Value::String("v9".to_string()),
                ],
                vec![
                    Value::Integer(4),
                    Value::Integer(4),
                    Value::Float(4.4),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v10".to_string()),
                    Value::String("v11".to_string()),
                    Value::String("v12".to_string()),
                ],
            ],
        )?;

        let res = s.execute("delete from t2 where d = false;")?;
        assert_eq!(res, ResultSet::Delete { count: 2 });
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![vec![
                Value::Integer(3),
                Value::Integer(3),
                Value::Float(3.3),
                Value::Boolean(true),
                Value::Boolean(false),
                Value::String("v7".to_string()),
                Value::String("v8".to_string()),
                Value::String("v9".to_string()),
            ]],
        )?;

        let res = s.execute("delete from t2;")?;
        assert_eq!(res, ResultSet::Delete { count: 1 });
        scan_table_and_compare(&mut s, "t2", vec![])?;

        Ok(())
    }

    #[test]
    fn test_sort() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
        s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
        s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
        s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
        s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
        s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

        match s.execute("select * from t3 order by b, c desc;")? {
            ResultSet::Scan { columns, rows } => {
                for r in rows {
                    println!("{:?}", r);
                }
            }
            _ => unreachable!(),
        }

    
        Ok(())
    }
}