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

        // 找到表中的主键作为一行数据的唯一标识
        let pk = table.get_primary_key(&row)?;
        // 查看主键对应的数据是否已经存在了
        let id = Key::Row(table_name.clone(), pk.clone()).encode()?;
        // 校验主键唯一性
        if self.txn.get(id.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "Duplicate data for primary key {} in table {}",
                pk, table_name
            )));
        }

        // 存放数据
        let value = bincode::serialize(&row)?;
        self.txn.set(id, value)?;


        Ok(())
    }

    // 更新行
    // 如果有主键更新，删除原来的数据，新增一条新的数据
    // 否则就 table_name + primary key => 更新数据
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;
        // 如果更新了主键，则删掉原有数据
        if *id != new_pk {
            let oldKey = Key::Row(table.name.clone(), id.clone()).encode()?;
            self.txn.delete(oldKey)?;
        }
        let key = Key::Row(table.name.clone(), new_pk.clone()).encode()?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;

        Ok(())
    }

    fn scan_table(
        &self,
        table_name: String,
        filter: Option<(String, Expression)>,
    ) -> Result<Vec<Row>> {
        // 由于要查找表的所有数据，所以就可以用前缀扫描，
        // 只扫描表名就可以实现需求了，所以就创建一个枚举
        let prefix = KeyPrefix::Row(table_name.clone()).encode()?;
        let table = self.must_get_table(table_name)?;
        let results = self.txn.scan_prefix(prefix)?;
        // 将Vec<ScanResult>变成Vec<Row>格式
        let mut rows = Vec::new();
        for result in results {
            // 过滤数据
            let row: Row = bincode::deserialize(&result.value)?; // ScanResult由key和value组成，这里要的是value
            if let Some((col, expr)) = &filter {
                // 查看要筛选的列在表中是第几列
                let col_index = table.get_col_index(&col)?;
                // 如果表达式值相同，则说明是筛选结果
                if Value::from_expression(expr.clone()) == row[col_index] {
                    rows.push(row);
                }
            } else {
                // 说明不筛选，全要
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
    Table(String),  // 表，参数是表名
    Row(String, Value), // 行， 参数是表名+主键的值
}

// 与之前的道理相同，String是变长的
// 为了前缀能匹配的上，所以用自己实现的序列化方法
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
}
