use serde::{Deserialize, Serialize};

use crate::sql::types::{DataType, Value};

/// Table schema definition
#[derive(Debug, PartialEq, Serialize, Deserialize)] 
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// Column schema definition
#[derive(Debug, PartialEq, Serialize, Deserialize)] 
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}
