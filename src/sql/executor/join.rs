use crate::{
    error::{Error, Result},
    sql::engine::Transaction,
};

use super::{Executor, ResultSet};

/// Nested Loop Join executor - produces Cartesian product of two tables
pub struct NestedLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { left, right })
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
            let mut new_cols = lcols;
            // Execute right side
            if let ResultSet::Scan {
                columns: rcols,
                rows: rrows,
            } = self.right.execute(txn)?
            {
                // Extend columns
                new_cols.extend(rcols);

                // Nested loop: produce Cartesian product
                for lrow in &lrows {
                    for rrow in &rrows {
                        // Extend row
                        let mut row = lrow.clone();
                        row.extend(rrow.clone());
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