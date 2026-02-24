use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, sql::types::{DataType, Row, Value}};

/// Table schema definition
#[derive(Debug, PartialEq, Serialize, Deserialize)] 
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    /// Validates table schema
    pub fn validate(&self) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                self.name
            )));
        }

        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => {
                return Err(Error::Internal(format!(
                    "No primary key for table {}",
                    self.name
                )))
            }
            _ => {
                return Err(Error::Internal(format!(
                    "Multiple primary keys for table {}",
                    self.name
                )))
            }
        }

        // Validate column constraints
        for col in &self.columns {
            // Primary key cannot be nullable
            if col.primary_key && col.nullable {
                return Err(Error::Internal(format!(
                    "Primary key {} cannot be nullable in table {}",
                    col.name, self.name
                )));
            }
            // Validate default value type matches column type
            if let Some(default_val) = &col.default {
                match default_val.datatype() {
                    Some(dt) => {
                        if dt != col.datatype {
                            return Err(Error::Internal(format!(
                                "Default value for column {} mismatch in table {}",
                                col.name, self.name
                            )));
                        }
                    }
                    None => {}
                }
            }
        }

        Ok(())
    }

    /// Extracts primary key value from a row
    pub fn get_primary_key(&self, row: &Row) -> Result<Value> {
        let pos = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .expect("No primary key found");
        Ok(row[pos].clone())
    }

    /// Returns the column index for a given column name
    pub fn get_col_index(&self, col_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or(Error::Internal(format!("column {} not found", col_name)))
    }
}

/// Column schema definition
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    /// Whether this column is the primary key
    pub primary_key: bool,
}
