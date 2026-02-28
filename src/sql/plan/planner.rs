use crate::{error::{Error, Result}, sql::{parser::ast::{self, Expression}, plan::{Node, Plan}, schema::{self, Table}, types::Value}};

/// Query planner - converts AST into execution plan nodes
pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    /// Builds an execution plan from an AST statement
    pub fn build(&mut self, stmt: ast::Statement) -> Result<Plan> {
        Ok(Plan(self.build_statement(stmt)?))
    }

    pub fn build_statement(&self, stmt: ast::Statement) -> Result<Node> {
        Ok(match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.primary_key);
                            let default = match c.default {
                                Some(expr) => Some(Value::from_expression(expr)),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };

                            schema::Column {
                                name: c.name,
                                datatype: c.datatype,
                                nullable,
                                default,
                                primary_key: c.primary_key,
                            }
                        })
                        .collect(),
                },
            },
            ast::Statement::Insert { table_name, columns, values } => Node::Insert {
                table_name,
                columns: columns.unwrap_or_default(),
                values,
            },
            ast::Statement::Select {
                select,
                from,
                // WHERE clause - should be an Operation variant (e.g., Equal, GreaterThan, LessThan)
                // not a Function variant
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                // Build scan node from FROM clause (single table or join result)
                // Also determines the Scan filter condition
                let mut node = self.build_from_item(from, &where_clause)?;

                // aggregate - detect aggregate functions in select expressions、group by
                let mut has_agg = false;
                if !select.is_empty() {
                    for (expr, _) in select.iter() {
                        if let ast::Expression::Function(_, _) = expr {
                            has_agg = true;
                            break;
                        }
                    }
                    if group_by.is_some() {
                        has_agg = true;
                    }
                    if has_agg {
                        node = Node::Aggregate {
                            source: Box::new(node),
                            exprs: select.clone(),
                            group_by,
                        }
                    }
                }

                // having
                if let Some(expr) = having {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: expr,
                    }
                }

                if !order_by.is_empty() {
                    node = Node::Order {
                        source: Box::new(node),
                        order_by,
                    }
                }

                // OFFSET - must be processed before LIMIT when both are present
                if let Some(expr) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: match Value::from_expression(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid offset".into())),
                        },
                    }
                }

                // LIMIT
                if let Some(expr) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: match Value::from_expression(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid limit".into())),
                        },
                    }
                }
                
                // projection - current design: projection and aggregate are mutually exclusive
                //
                // Note: The following SQL will have issues without GROUP BY support:
                //   SELECT name, COUNT(*) FROM users GROUP BY name;
                //   Expected: name | count
                //   Actual: only count
                // GROUP BY implementation needed to handle non-aggregate columns properly.
                if !select.is_empty() && !has_agg {
                    node = Node::Projection {
                        source: Box::new(node),
                        exprs: select,
                    }
                }

                node
            },
            ast::Statement::Update {
                table_name,
                columns,
                where_clause,
            } => Node::Update {
                table_name: table_name.clone(),
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
                columns,
            },
            ast::Statement::Delete {
                table_name,
                where_clause,
            } => Node::Delete {
                table_name: table_name.clone(),
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
            },
        })
    }

    fn build_from_item(&self, item: ast::FromItem, filter: &Option<Expression>) -> Result<Node> {
        Ok(match item {
            ast::FromItem::Table { name } => Node::Scan { 
                table_name: name, 
                filter: filter.clone(),
            },
            ast::FromItem::Join { 
                left, 
                right, 
                join_type ,
                predicate,
            } =>  {
                // For RIGHT JOIN, swap left and right to avoid duplicate code
                let (left, right) = match join_type {
                    ast::JoinType::Right => (right, left),
                    _ => (left, right),
                };

                let outer = match join_type {
                    ast::JoinType::Cross | ast::JoinType::Inner => false,
                    _ => true, // LEFT and RIGHT joins are both outer joins
                };

                Node::NestedLoopJoin {
                    // Recursively build join nodes (base case: single table)
                    left: Box::new(self.build_from_item(*left, filter)?),
                    right: Box::new(self.build_from_item(*right, filter)?),
                    predicate,
                    outer,
                }
            },
        })
    }
}
