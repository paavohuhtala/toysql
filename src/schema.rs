#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ColumnType {
  Null,
  Int32,
  Int64,
}

#[derive(Debug)]
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
  pub nullable: bool,
}

impl ColumnSchema {
  pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
    ColumnSchema {
      name: name.into(),
      column_type,
      nullable: false,
    }
  }

  pub fn as_nullable(mut self) -> Self {
    self.nullable = true;
    self
  }
}

#[derive(Debug)]
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
