use crate::{error::{Error, Result}, sql::{parser::ast, plan::{Node, Plan}, schema::{self, Table}, types::Value}};

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
                order_by,
                limit,
                offset,
            } => {
                // Build scan node from FROM clause (single table or join result)
                let mut node = self.build_from_item(from)?;

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
                
                // projection
                if !select.is_empty() {
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

    fn build_from_item(&self, item: ast::FromItem) -> Result<Node> {
        Ok(match item {
            ast::FromItem::Table { name } => Node::Scan { 
                table_name: name, 
                filter: None 
            },
            ast::FromItem::Join {
                left,
                right,
                join_type
            } => match join_type {
                ast::JoinType::Cross => Node::NestedLoopJoin {
                    // Recursively build join nodes (base case: single table)
                    left: Box::new(self.build_from_item(*left)?),
                    right: Box::new(self.build_from_item(*right)?),
                },
                _ => todo!(),
            },
        })
    }
}
