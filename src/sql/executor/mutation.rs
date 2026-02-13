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

            println!("insert row: {:?}", insert_row);
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert { count })
    }
}
