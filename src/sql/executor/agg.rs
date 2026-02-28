use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::{self, Expression},
        types::Value,
    },
};

use super::{Executor, ResultSet};

/// Aggregate executor for COUNT, SUM, MIN, MAX, AVG functions
///
/// Supports optional GROUP BY clause for grouping rows before aggregation.
/// Without GROUP BY, the entire input is treated as a single group.
pub struct Aggregate<T: Transaction> {
    source: Box<dyn Executor<T>>,
    exprs: Vec<(Expression, Option<String>)>,
    group_by: Option<Expression>,
}

impl<T: Transaction> Aggregate<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        exprs: Vec<(Expression, Option<String>)>,
        group_by: Option<Expression>,
    ) -> Box<Self> {
        Box::new(Self {
            source,
            exprs,
            group_by,
        })
    }
}

impl<T: Transaction> Executor<T> for Aggregate<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Scan { columns, rows } = self.source.execute(txn)? {
            let mut new_cols = Vec::new();
            let mut new_rows = Vec::new();

            // Compute aggregate values for a group of rows
            let mut calc = |col_val: Option<&Value>, rows: &Vec<Vec<Value>>| -> Result<Vec<Value>> {
                let mut new_row = Vec::new();
                for (expr, alias) in &self.exprs {
                    match expr {
                        ast::Expression::Function(func_name, col_name) => {
                            let calculator = <dyn Calculator>::build(&func_name)?;
                            let val = calculator.calc(&col_name, &columns, rows)?;

                            // Use alias if provided, otherwise use function name
                            if new_cols.len() < self.exprs.len() {
                                new_cols.push(if let Some(a) = alias {
                                    a.clone()
                                } else {
                                    func_name.clone()
                                });
                            }
                            new_row.push(val);
                        }
                        // Group key column
                        ast::Expression::Field(col) => {
                            if self.group_by.is_none() {
                                return Err(Error::Internal(format!(
                                    "column {} must appear in GROUP BY or be used in aggregate function",
                                    col
                                )));
                            }
                            if let Some(ast::Expression::Field(group_col)) = &self.group_by {
                                if *col != *group_col {
                                    return Err(Error::Internal(format!(
                                        "{} must appear in the GROUP BY clause or aggregate function",
                                        col
                                    )));
                                }
                            }

                            if new_cols.len() < self.exprs.len() {
                                new_cols.push(if let Some(a) = alias {
                                    a.clone()
                                } else {
                                    col.clone()
                                });
                            }
                            new_row.push(col_val.unwrap().clone());
                        }
                        _ => return Err(Error::Internal("unexpected expression".into())),
                    }
                }
                Ok(new_row)
            };

            if let Some(ast::Expression::Field(group_col)) = &self.group_by {
                let pos = match columns.iter().position(|c| *c == *group_col) {
                    Some(pos) => pos,
                    None => {
                        return Err(Error::Internal(format!(
                            "group by column {} not in table",
                            group_col
                        )))
                    }
                };

                // Group rows by the group key
                let mut agg_map: HashMap<&Value, Vec<Vec<Value>>> = HashMap::new();
                for row in rows.iter() {
                    let key = &row[pos];
                    let value = agg_map.entry(key).or_insert(Vec::new());
                    value.push(row.clone());
                }

                for (key, group_rows) in agg_map {
                    let row = calc(Some(key), &group_rows)?;
                    new_rows.push(row);
                }
            } else {
                // No GROUP BY - aggregate entire table
                let row = calc(None, &rows)?;
                new_rows.push(row);
            }

            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: new_rows,
            });
        }
        Err(Error::Internal("Unexpected result set".into()))
    }
}

/// Trait for aggregate function implementations
///
/// Each aggregate function (COUNT, SUM, etc.) implements this trait
/// to compute its result from a set of values.
pub trait Calculator {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value>;
}

impl dyn Calculator {
    /// Runtime dispatch to appropriate calculator based on function name
    pub fn build(func_name: &String) -> Result<Box<dyn Calculator>> {
        Ok(match func_name.to_uppercase().as_ref() {
            "COUNT" => Count::new(),
            "SUM" => Sum::new(),
            "MIN" => Min::new(),
            "MAX" => Max::new(),
            "AVG" => Avg::new(),
            _ => return Err(Error::Internal("unknown aggregate function".into())),
        })
    }
}

/// COUNT aggregate function
pub struct Count;

impl Count {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Count {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        let mut count = 0;
        for row in rows.iter() {
            if row[pos] != Value::Null {
                count += 1;
            }
        }
        Ok(Value::Integer(count))
    }
}

/// MIN aggregate function
pub struct Min;

impl Min {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Min {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        let mut min_val = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            min_val = values[0].clone();
        }
        Ok(min_val)
    }
}

/// MAX aggregate function
pub struct Max;

impl Max {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Max {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        let mut max_val = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            max_val = values[values.len() - 1].clone();
        }
        Ok(max_val)
    }
}

/// SUM aggregate function
pub struct Sum;

impl Sum {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Sum {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        let mut sum = None;
        for row in rows.iter() {
            match row[pos] {
                Value::Null => {}
                Value::Integer(v) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    sum = Some(sum.unwrap() + v as f64);
                }
                Value::Float(v) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    sum = Some(sum.unwrap() + v);
                }
                _ => return Err(Error::Internal(format!("can not calc column {}", col_name))),
            }
        }

        Ok(match sum {
            Some(s) => Value::Float(s),
            None => Value::Null,
        })
    }
}

/// AVG aggregate function
pub struct Avg;

impl Avg {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Avg {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let sum = Sum::new().calc(col_name, cols, rows)?;
        let count = Count::new().calc(col_name, cols, rows)?;
        Ok(match (sum, count) {
            (Value::Float(s), Value::Integer(c)) => Value::Float(s / c as f64),
            _ => Value::Null,
        })
    }
}