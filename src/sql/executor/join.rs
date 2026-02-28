use crate::{
    error::{Error, Result},
    sql::{engine::Transaction, parser::ast::{self, Expression, evaluate_expr}, types::Value},
};

use super::{Executor, ResultSet};

/// Nested Loop Join executor - produces Cartesian product of two tables
pub struct NestedLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // Execute left side first
        if let ResultSet::Scan {
            columns: lcols,
            rows: lrows,
        } = self.left.execute(txn)?
        {
            let mut new_rows = Vec::new();
            let mut new_cols = lcols.clone();
            // Execute right side
            if let ResultSet::Scan {
                columns: rcols,
                rows: rrows,
            } = self.right.execute(txn)?
            {
                // Extend columns
                new_cols.extend(rcols.clone());

                // Nested loop: for each left row, iterate through all right rows
                for lrow in &lrows {
                    let mut matched = false;
                    for rrow in &rrows {
                        let mut row = lrow.clone();

                        if let Some(expr) = &self.predicate {
                            match evaluate_expr(expr, &lcols, lrow, &rcols, rrow)? {
                                Value::Null => {}
                                Value::Boolean(false) => {}
                                Value::Boolean(true) => {
                                    row.extend(rrow.clone());
                                    new_rows.push(row);
                                    matched = true;
                                }
                                _ => return Err(Error::Internal("Unexpected expression".into())),
                            }
                        } else {
                            // No predicate means CROSS JOIN
                            row.extend(rrow.clone());
                            new_rows.push(row);
                        }
                    }

                    // For outer joins, fill with NULL if no match found
                    if self.outer && !matched {
                        let mut row = lrow.clone();
                        for _ in 0..rrows[0].len() {
                            row.push(Value::Null);
                        }
                        new_rows.push(row);
                    }
                }
            }
            /*
            Note: When two tables have duplicate column names in a CROSS JOIN,
            the result will have duplicate column names.
            Different databases handle this differently:
            - MySQL: Allows duplicates, later columns shadow earlier ones
            - PostgreSQL: Allows duplicates, requires table qualification
            - SQLite: Allows duplicates

            For better handling:
            1. Store table names in NestedLoopJoin
            2. Generate prefixed column names (e.g., users.id, orders.id)
            3. Support table.column syntax in Projection
            */
            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: new_rows,
            });
        }
        Err(Error::Internal("Unexpected result set".into()))
    }
}

