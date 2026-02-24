use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, sql::types::{DataType, Row, Value}};

/// Table schema definition
#[derive(Debug, PartialEq, Serialize, Deserialize)] 
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    // 校验表的有效性
    pub fn validate(&self) -> Result<()> {
        // 校验是否有列信息
        if self.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                self.name
            )));
        }

        // 校验是否有主键
        // 是主键的才不会被过滤出去
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {} // 1个主键是符合预期的
            0 => {
                return Err(Error::Internal(format!(
                    "No primary key for table {}",
                    self.name
                )))
            }
            _ => {
                return Err(Error::Internal(format!(
                    "Multiple primary keys for table {}",
                    self.name
                )))
            }
        }

        Ok(())
    }

    // 获取主键的值，如下面的1
    // id age name
    // 1   10  mike
    pub fn get_primary_key(&self, row: &Row) -> Result<Value> {
        // 建表语句与实际插入到kv的顺序是一样的
        let pos = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .expect("No primary key found");
        Ok(row[pos].clone())
    }

    // 获取指定列名在表中是第几列
    pub fn get_col_index(&self, col_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or(Error::Internal(format!("column {} not found", col_name)))
    }
}

/// Column schema definition
#[derive(Debug, PartialEq, Serialize, Deserialize)] 
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool, // 是否为主键
}
