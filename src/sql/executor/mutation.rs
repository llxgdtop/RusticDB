use std::collections::{BTreeMap, HashMap};

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

/// Pads row with default values for unspecified columns
///
/// Used when INSERT specifies values without column names.
/// Fills remaining columns with their default values.
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }
    Ok(results)
}

/// Builds a row with values mapped to specified columns
///
/// Used when INSERT specifies column names.
/// Maps input values to columns and fills unspecified columns with defaults.
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    if columns.len() != values.len() {
        return Err(Error::Internal(format!("columns and values num mismatch")));
    }

    let mut inputs = HashMap::new();
    for (i, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, values[i].clone());
    }

    let mut results = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(value) = &col.default {
            results.push(value.clone());
        } else {
            return Err(Error::Internal(format!(
                "No value given for the column {}",
                col.name
            )));
        }
    }

    Ok(results)
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_get_table(self.table_name.clone())?;
        let mut count = 0;

        for exprs in self.values {
            let row: Row = exprs.into_iter()
                .map(Value::from_expression)
                .collect();

            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                make_row(&table, &self.columns, &row)?
            };

            // println!("insert row: {:?}", insert_row);
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert { count })
    }
}

// Update 执行器
pub struct Update<T: Transaction> {
    table_name: String,
    // 由于要有一个扫描执行器，所以要用trait对象，所以也和mod.rs一样加一个范型参数
    source: Box<dyn Executor<T>>, 
    columns: BTreeMap<String, Expression>,
}

impl<T: Transaction> Update<T> {
    pub fn new(
        table_name: String,
        source: Box<dyn Executor<T>>,
        columns: BTreeMap<String, Expression>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            source,
            columns,
        })
    }
}

impl<T: Transaction> Executor<T> for Update<T> {
    fn execute(self: Box<Self>, txn:&mut T) -> Result<ResultSet> {
        let mut count = 0;
        // 执行扫描操作，获取扫描结果即一行行经过where筛选的数据
        match self.source.execute(txn)? {
            // 这些数据是每一个列名以及对应的行的值
            ResultSet::Scan { columns, rows } => {
                let table = txn.must_get_table(self.table_name)?;
                // 遍历所有需要更新的行
                for row in rows {
                    let mut new_row = row.clone();
                    // 获取此行的主键值，用于update_row里面判断主键是否需要更新
                    let pk = table.get_primary_key(&row)?;

                    // 针对每一行，看每一列是否在需要更新的columns列表当中
                    // columns.iter()这个是扫描结果的columns，而self.columns是Update执行器结构体的字段
                    // 就是遍历前者的每一列，看它是否出现在BTreeMap当中，如果有就是需要更新
                    for (i, col) in columns.iter().enumerate() {
                        if let Some(expr) = self.columns.get(col) {
                            new_row[i] = Value::from_expression(expr.clone());
                        }
                    }
                    // 执行更新操作
                    txn.update_row(&table, &pk, new_row)?;
                    count += 1;
                }
            },
            // 只想获取扫描的结果，别的结果就报错
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
        Ok(ResultSet::Update { count })
    }
}