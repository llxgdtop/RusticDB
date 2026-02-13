use std::collections::HashMap;

use crate::{error::{Error, Result}, sql::{engine::Transaction, executor::ResultSet, parser::ast::Expression, schema::Table, types::{Row, Value}}};

use super::Executor;

/// INSERT executor
pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

// 列对齐
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c          d
// 1       2       3      default 填充
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    // 获取表的列信息，然后转成迭代器，跳过要明确插入值的那几列(如上方的a,b,c)
    for column in table.columns.iter().skip(row.len()) {
        // 看剩余要填充的列是否有默认值
        if let Some(default) = &column.default { // 不加借用会触发所有权转移
            results.push(default.clone()); 
        }else {
            return Err(Error::Internal(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }
    Ok(results)
}

// 插入行
// tbl:
// insert into tbl(d, c) values(1, 2);
//    a          b       c          d
// default   default     2          1
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // 判断列数是否和value数一致
    if columns.len() != values.len() {
        return Err(Error::Internal(format!("columns and values num mismatch")));
    }

    let mut inputs = HashMap::new();
    // 把要插入的列给收集起来
    for (i, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, values[i].clone()); // 比如上面的d的值是1
    }

    let mut results = Vec::new();
    // 遍历所有列
    for col in table.columns.iter() {
        // 先从hash表里找看它是否有要插入的值
        if let Some(value) = inputs.get(&col.name){
            results.push(value.clone()); // 有。这里必须要clone()，否则result的类型是Vec<&Value>
        }else if let Some(value) = &col.default {
            results.push(value.clone()); // 没有，但有默认值
        }else {
            return Err(Error::Internal(format!( // 没有并且没有默认值
                "No value given for the column {}",
                col.name
            )));
        }
    }

    Ok(results)
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 先取出表信息
        let table = txn.must_get_table(self.table_name.clone())?;
        let mut count = 0; // 影响行数
        // 将表达式集合转成每一行的数据
        for exprs in self.values {
            let row = exprs.into_iter()
                .map(|e| Value::from_expression(e))
                .collect::<Vec<_>>(); // 将迭代器收集回成一个Value数组

            // 如果没有指定需要插入的列，则需要对齐
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            }else {
            // 指定了插入的列，则需要对这一行的value数据做整理
                make_row(&table, &self.columns, &row)?
            };

            // 调用底层txn插入数据
            println!("insert row: {:?}", insert_row);
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert { count })
    }
}
