use std::{cmp::Ordering, collections::HashMap};

use crate::{error::{Error, Result}, sql::{engine::Transaction, executor::ResultSet, parser::ast::{Expression, OrderDirection}}};

use super::Executor;

/// Table scan executor (SELECT)
pub struct Scan {
    table_name: String,
    filter: Option<(String, Expression)>,
}

impl Scan {
    pub fn new(table_name: String, filter: Option<(String, Expression)>) -> Box<Self> {
        Box::new(Self { table_name, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self:Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_get_table(self.table_name.clone())?;
        let rows = txn.scan_table(self.table_name.clone(), self.filter)?;
        Ok(ResultSet::Scan { 
            columns: table.columns.into_iter().map(|c| c.name.clone()).collect(), 
            rows 
        })
    }
}

/// ORDER BY executor - sorts rows by specified columns
pub struct Order<T: Transaction> {
    source: Box<dyn Executor<T>>,
    order_by: Vec<(String, OrderDirection)>,
}

impl<T: Transaction> Order<T> {
    pub fn new(source: Box<dyn Executor<T>>, order_by: Vec<(String, OrderDirection)>) -> Box<Self> {
        Box::new(Self { source, order_by })
    }
}

impl<T: Transaction> Executor<T> for Order<T> {
    fn execute(self: Box<Self>, txn:&mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, mut rows } => {
                // Map ORDER BY column positions to actual table column positions
                // e.g., "ORDER BY c, a, b" where table columns are [a, b, c]
                let mut order_col_index = HashMap::new();
                for (i, (col_name, _)) in self.order_by.iter().enumerate() {
                    match columns.iter().position(|c| *c == *col_name) {
                        Some(pos) => order_col_index.insert(i, pos),
                        None => {
                            return Err(Error::Internal(format!(
                                "order by column {} is not in table",
                                col_name
                            )))
                        }
                    };
                }

                // Multi-column sort: compare rows column by column according to ORDER BY clause
                // - If comparison is Equal, continue to next column
                // - If Less/Greater, apply ASC/DESC direction and return
                // - If types are incomparable (None), continue to next column
                rows.sort_by(|col1, col2| {
                    for (i, (_, direction)) in self.order_by.iter().enumerate() {
                        let col_index = order_col_index.get(&i).unwrap();
                        let x = &col1[*col_index];
                        let y = &col2[*col_index];
                        match x.partial_cmp(y) {
                            Some(Ordering::Equal) => {}
                            Some(o) => {
                                return if *direction == OrderDirection::Asc {
                                    o
                                } else {
                                    o.reverse()
                                }
                            }
                            None => {}
                        }
                    }
                    Ordering::Equal
                });

                Ok(ResultSet::Scan { columns, rows })
            }
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
    }
}

/// LIMIT executor - restricts the number of rows returned
pub struct Limit<T: Transaction> {
    source: Box<dyn Executor<T>>,
    limit: usize,
}

impl<T: Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: usize) -> Box<Self> {
        Box::new(Self { source, limit })
    }
}

impl<T: Transaction> Executor<T> for Limit<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => Ok(ResultSet::Scan {
                columns,
                rows: rows.into_iter().take(self.limit).collect(),
            }),
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
    }
}

/// OFFSET executor - skips the first N rows
pub struct Offset<T: Transaction> {
    source: Box<dyn Executor<T>>,
    offset: usize,
}

impl<T: Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: usize) -> Box<Self> {
        Box::new(Self { source, offset })
    }
}

impl<T: Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => Ok(ResultSet::Scan {
                columns,
                rows: rows.into_iter().skip(self.offset).collect(),
            }),
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
    }
}