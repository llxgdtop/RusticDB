use crate::{
    error::{Error, Result},
    sql::{engine::Transaction, parser::ast::{self, Expression}, types::Value},
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

                // Nested loop: produce Cartesian product
                // 对每一行左表数据，遍历所有右表数据  
                for lrow in &lrows {
                    let mut matched = false; // 标记左表的这一行有无匹配上
                    for rrow in &rrows {
                        // 扩展行
                        let mut row = lrow.clone();

                        // 如果有Join条件
                        if let Some(expr) = &self.predicate {
                            match evaluate_expr(expr, &lcols, lrow, &rcols, rrow)? {
                                Value::Null => {}
                                Value::Boolean(false) => {}
                                Value::Boolean(true) => {
                                    // 满足匹配条件则扩展行
                                    row.extend(rrow.clone());
                                    new_rows.push(row);
                                    matched = true;
                                }
                                _ => return Err(Error::Internal("Unexpected expression".into())),
                            }
                        }else {
                            // 说明没有Join条件，为Cross Join
                            row.extend(rrow.clone());
                            new_rows.push(row);
                        }
                        
                        
                    }

                    // 如果是左右连接（外连接），且没有匹配到任何右表数据用NULL填充
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

// 表达式求值。对于当前来说，就是
// 1.求某一行某一列的值
// 2.对某两行中的相同位置的两列比较是否相等
fn evaluate_expr(
    expr: &Expression,
    lcols: &Vec<String>, // 左表列名
    lrows: &Vec<Value>, // 左表当前行数据
    rcols: &Vec<String>, // 右表列名
    rrows: &Vec<Value>, // 右表当前行数据
) -> Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            let pos = match lcols.iter().position(|c| *c == *col_name) {
                Some(pos) => pos,
                None => {
                    return Err(Error::Internal(format!(
                        "column {} is not in table",
                        col_name
                    )))
                }
            };
            Ok(lrows[pos].clone())
        },
        Expression::Operation(operation) => match operation {
            ast::Operation::Equal(lexpr, rexpr) => {
                // 递归计算左边的表达式的值（即指定的某一行中某一列的值）
                let lv = evaluate_expr(&lexpr, lcols, lrows, rcols, rrows)?;
                // 递归计算右边的表达式的值（即指定的某一行中某一列的值），注意上方Field分支中，使用lcols去求得值的，所以这里要交换一下参数
                let rv = evaluate_expr(&rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    // 用true和false表示是否相等
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare exression {} and {}",
                            l, r
                        )))
                    }
                })
            }
        },
        _ => return Err(Error::Internal("unexpected expression".into())), // 对于常量求值，在types.rs中
    }
}