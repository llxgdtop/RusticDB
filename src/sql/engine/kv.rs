use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::{
        schema::Table,
        types::{Row, Value},
    },
    storage::{self, engine::Engine as StorageEngine},
};

use super::{Engine, Transaction};

// KV Engine的定义
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
        // 实际上就是调用下面的new方法，然后下面的new方法需要一个mvcc transaction的参数，所以
        // 又要传入这个类型的参数，而属性kv就是这个类型，直接调用begin方法即可传入参数
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// KVTransaction实际上是对底层的MVCC Transaction的封装
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
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    // 插入数据
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 校验行的有效性，即看插入的数据类型和表的格式是否相同
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {} // 未指定插入这一列但这一列可为空
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )))
                }
                Some(dt) if dt != col.datatype => {  // 指定插入这一列但数据类型不匹配
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                }
                _ => {}
            }
        }

        // 存放数据
        // key是表名+第一列的值，value是整个row的数据进行编码
        // 暂时以第一列作为主键，一行数据的唯一标识，todo
        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;

        Ok(())
    }

    // 获取数据
    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        // 当前只支持select *，在插入数据的时候key为表名+第一列的值，由于要查找表的所有数据，
        // 所以就可以用前缀扫描，只扫描表名就可以实现需求了，所以就创建一个枚举
        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;
        // 将Vec<ScanResult>变成Vec<Row>格式
        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?; // ScanResult由key和value组成，这里要的是value
            rows.push(row);
        }
        Ok(rows)
    }

    // 创建表
    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否已经存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }

        // 判断表的有效性
        if table.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                table.name
            )));
        }

        // key为表名，value为列的属性数据(如列名、是否为空、是否有默认值等等)
        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?; 
        self.txn.set(bincode::serialize(&key)?, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        Ok(self
            .txn
            .get(bincode::serialize(&key)?)? // 序列化会返回一个Result，所以里面要用一个?，加上get也会返回Result，所以外面还要用?
            .map(|v| bincode::deserialize(&v)) // 有值就用map反序列化取出来
            .transpose()?) // 再转成Result，这里外面加个Ok包裹是因为transpose转成了Result<Option>,通过?解了里面的Option出来,但最后要返回Result，所以还要用一个Ok包裹
            /*
            map这里得到的数据类型是Option<Result<Table, Box<bincode::ErrorKind>>>，有个box是bincode的设计
            代码使用了 .transpose() 方法把 Option<Result<T, E>> 转换成 Result<Option<T>,
            E>，然后外层的 ? 通过 error.rs:32-36 中定义的 From<Box<ErrorKind>> trait
            实现，把 Box<ErrorKind> 转换成了自定义的 Error 类型。所以最终返回类型仍然是 Result<Option<Table>>（即 Result<Option<Table>,Error>）
             */
    }
}

// 定义枚举来区分操作对象
// 底层是kv存储，对于表操作，k是Table(表名)
// 对于行操作，k是Row(表名， 值)
#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),  // 表，参数是表名
    Row(String, Value), // 行， 参数是表名+第一列的值（当前这么做不具备唯一性，所以后续要改成表名+主键的方式）
}

// 在 bincode 中，枚举的序列化格式是：[变体索引] [变体数据...]
// 变体索引按照枚举中定义的顺序，从 0 开始编号。所以要与上面这个枚举来做对齐，可以见学习文档
#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        let v1 = s.execute("select * from t1;")?;
        println!("{:?}",v1);
        Ok(())
    }
}