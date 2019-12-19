use crate::common::{ColumnType, Value, ValueType};

#[derive(Debug, Clone)]
pub struct BTreeIndexSchema {
  pub column_index: usize,
  pub is_unique: bool,
}

impl BTreeIndexSchema {
  pub fn new(column_index: usize) -> BTreeIndexSchema {
    BTreeIndexSchema {
      column_index,
      is_unique: true,
    }
  }
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
  pub name: String,
  pub column_type: ColumnType,
}

impl ColumnSchema {
  pub fn new(name: impl Into<String>, column_type: ValueType) -> Self {
    ColumnSchema {
      name: name.into(),
      column_type: ColumnType {
        base_type: column_type,
        is_nullable: false,
      },
    }
  }

  pub fn as_nullable(mut self) -> Self {
    self.column_type.is_nullable = true;
    self
  }
}

#[derive(Debug, Clone)]
pub struct TableSchema {
  name: String,
  columns: Vec<ColumnSchema>,
  indices: Vec<BTreeIndexSchema>,
}

impl TableSchema {
  pub fn new(name: impl Into<String>, columns: &[ColumnSchema]) -> Self {
    TableSchema {
      name: name.into(),
      columns: columns.to_vec(),
      indices: Vec::new(),
    }
  }

  pub fn name(&self) -> &str {
    &self.name
  }

  pub fn columns(&self) -> &[ColumnSchema] {
    &self.columns
  }

  pub fn indices(&self) -> &[BTreeIndexSchema] {
    &self.indices
  }

  pub fn with_index(mut self, index: BTreeIndexSchema) -> Self {
    self.indices.push(index);
    self
  }
}

#[derive(Clone)]
pub struct DatabaseSchema {
  tables: Vec<TableSchema>,
}

impl DatabaseSchema {
  pub fn new(tables: &[TableSchema]) -> Self {
    DatabaseSchema {
      tables: tables.to_vec(),
    }
  }

  pub fn tables(&self) -> &[TableSchema] {
    &self.tables
  }
}
