use crate::sql::{parser::ast, plan::{Node, Plan}, schema::{self, Table}, types::Value};

/// Converts AST statements into execution plan nodes
pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    /// Builds an execution plan from an AST statement
    pub fn build(&mut self, stmt: ast::Statement) -> Plan {
        Plan(self.build_statement(stmt))
    }

    /// Converts an AST statement into an execution node
    ///
    /// Separated from `build` to allow for future optimizations,
    /// logging, or performance statistics without changing the build API.
    pub fn build_statement(&self, stmt: ast::Statement) -> Node {
        match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(true);
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
            ast::Statement::Select { table_name } => Node::Scan { table_name },
        }
    }
}
