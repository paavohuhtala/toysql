#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ColumnType {
  Null,
  Int32,
  Int64,
}

#[derive(Debug)]
pub struct BTreeIndexSchema {
  column_index: usize,
  is_unique: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
  pub name: String,
  pub column_type: ColumnType,
  pub nullable: bool,
}

impl ColumnSchema {
  pub fn new(name: impl Into<String>, column_type: ColumnType) -> ColumnSchema {
    ColumnSchema {
      name: name.into(),
      column_type,
      nullable: false,
    }
  }

  fn as_nullable(self) -> ColumnSchema {
    ColumnSchema {
      nullable: true,
      ..self
    }
  }
}

#[derive(Debug)]
pub struct TableSchema {
  name: String,
  columns: Vec<ColumnSchema>,
  indices: Vec<BTreeIndexSchema>,
}

impl TableSchema {
  pub fn new(name: impl Into<String>, columns: &[ColumnSchema]) -> TableSchema {
    TableSchema {
      name: name.into(),
      columns: columns.to_vec(),
      indices: Vec::new(),
    }
  }
}

impl TableSchema {
  pub fn columns(&self) -> &[ColumnSchema] {
    &self.columns
  }
}
