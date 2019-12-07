use std::collections::{BTreeMap, BTreeSet};
use std::iter::Iterator;

#[derive(Debug)]
enum SchemaError<'a> {
  InvalidColumn {
    index: usize,
    value: CellData,
    column: &'a ColumnSchema,
    table: &'a TableSchema,
  },
  InvalidRow {
    length: usize,
    table: &'a TableSchema,
  },
}

#[derive(Debug)]
struct TableSchema {
  name: String,
  columns: Vec<ColumnSchema>,
  indices: Vec<BTreeIndexSchema>,
}

#[derive(Debug)]
struct BTreeIndexSchema {
  column_index: usize,
  is_unique: bool,
}

impl TableSchema {
  pub fn check_row(&self, row: &[CellData]) -> Result<(), SchemaError> {
    if row.len() != self.columns.len() {
      return Err(SchemaError::InvalidRow {
        length: row.len(),
        table: self,
      });
    }

    for (i, (cell, column)) in row.iter().zip(self.columns.iter()).enumerate() {
      let is_valid = match cell.type_of() {
        ColumnType::Null if !column.nullable => false,
        cell_type if column.column_type != cell_type => false,
        _ => true,
      };

      if !is_valid {
        return Err(SchemaError::InvalidColumn {
          index: i,
          value: cell.clone(),
          column,
          table: self,
        });
      }
    }

    Ok(())
  }
}

#[derive(Debug)]
struct ColumnSchema {
  name: String,
  column_type: ColumnType,
  nullable: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ColumnType {
  Null,
  Int32,
  Int64,
}

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
enum CellData {
  Null,
  Int32(i32),
  Int64(i64),
}

impl CellData {
  fn type_of(&self) -> ColumnType {
    match self {
      CellData::Null => ColumnType::Null,
      CellData::Int32(_) => ColumnType::Int32,
      CellData::Int64(_) => ColumnType::Int64,
    }
  }
}

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
struct Row<'a>(&'a [CellData]);

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord)]
struct RowNumber(usize);

struct BTreeIndex<'a> {
  schema: &'a BTreeIndexSchema,
  data: BTreeMap<CellData, RowNumber>,
}

struct TableData<'a> {
  schema: &'a TableSchema,
  data: Vec<CellData>,
  freed_rows: BTreeSet<RowNumber>,
  indices: Vec<BTreeIndex<'a>>,
}

impl<'a> TableData<'a> {
  pub fn rows(&self) -> impl Iterator<Item = Row> {
    let freed_rows = &self.freed_rows;

    self
      .data
      .chunks(self.schema.columns.len())
      .enumerate()
      .filter(move |(i, _)| !freed_rows.contains(&RowNumber(*i)))
      .map(|(_, data)| Row(data))
  }

  fn stride(&self) -> usize {
    self.schema.columns.len()
  }

  pub fn allocated_rows_len(&self) -> usize {
    (self.data.len() / self.stride())
  }

  pub fn rows_len(&self) -> usize {
    self.allocated_rows_len() - self.freed_rows.len()
  }

  fn row_data_offset(&self, RowNumber(i): RowNumber) -> usize {
    i * self.stride()
  }

  pub fn insert(&mut self, row: &[CellData]) -> Result<(), SchemaError> {
    self.schema.check_row(&row)?;

    let stride = self.stride();

    match self.freed_rows.iter().nth(0).cloned() {
      Some(row_number) => {
        let offset = self.row_data_offset(row_number);
        self.data.as_mut_slice()[offset..offset + stride].clone_from_slice(row);
        self.freed_rows.remove(&row_number);
      }
      None => self.data.extend_from_slice(row),
    }

    Ok(())
  }

  pub fn truncate(&mut self) {
    for index in &mut self.indices {
      index.data.clear();
    }

    for i in 0..self.allocated_rows_len() {
      debug_assert!(
        self.freed_rows.insert(RowNumber(i)),
        "This row hasn't been removed before"
      );
    }

    debug_assert_eq!(self.rows_len(), 0, "No live rows remaining");
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn insert_valid_row() {
    let table_schema = TableSchema {
      name: String::from("test_table"),
      columns: vec![ColumnSchema {
        name: String::from("a"),
        column_type: ColumnType::Int32,
        nullable: false,
      }],
      indices: vec![],
    };

    let mut table_data = TableData {
      schema: &table_schema,
      data: Vec::new(),
      indices: vec![],
      freed_rows: BTreeSet::new(),
    };

    table_data
      .insert(&mut vec![CellData::Int32(123)])
      .expect("Inserting a valid row should succeed.");
  }

  #[test]
  fn can_iterate_rows() {
    let table_schema = TableSchema {
      name: String::from("test_table"),
      columns: vec![ColumnSchema {
        name: String::from("a"),
        column_type: ColumnType::Int32,
        nullable: false,
      }],
      indices: vec![],
    };

    let table_data = TableData {
      schema: &table_schema,
      data: vec![CellData::Int32(123), CellData::Int32(124)],
      indices: vec![],
      freed_rows: BTreeSet::new(),
    };

    assert_eq!(
      vec![Row(&[CellData::Int32(123)]), Row(&[CellData::Int32(124)]),],
      table_data.rows().collect::<Vec<_>>()
    )
  }

  #[test]
  fn truncate_marks_rows_as_deleted() {
    let table_schema = TableSchema {
      name: String::from("test_table"),
      columns: vec![ColumnSchema {
        name: String::from("a"),
        column_type: ColumnType::Int32,
        nullable: false,
      }],
      indices: vec![],
    };

    let mut table_data = TableData {
      schema: &table_schema,
      data: vec![CellData::Int32(1), CellData::Int32(2)],
      indices: vec![],
      freed_rows: BTreeSet::new(),
    };

    table_data.truncate();

    assert_eq!(table_data.rows_len(), 0);
    assert_eq!(table_data.allocated_rows_len(), 2);
    assert_eq!(table_data.rows().count(), 0);
  }

  #[test]
  fn insert_truncate_insert() {
    let table_schema = TableSchema {
      name: String::from("test_table"),
      columns: vec![
        ColumnSchema {
          name: String::from("a"),
          column_type: ColumnType::Int32,
          nullable: false,
        },
        ColumnSchema {
          name: String::from("b"),
          column_type: ColumnType::Int64,
          nullable: false,
        },
      ],
      indices: vec![],
    };

    let mut table_data = TableData {
      schema: &table_schema,
      data: vec![],
      indices: vec![],
      freed_rows: BTreeSet::new(),
    };

    table_data
      .insert(&[CellData::Int32(1), CellData::Int64(2)])
      .unwrap();
    table_data
      .insert(&[CellData::Int32(3), CellData::Int64(4)])
      .unwrap();

    table_data.truncate();

    table_data
      .insert(&[CellData::Int32(5), CellData::Int64(6)])
      .unwrap();
    table_data
      .insert(&[CellData::Int32(7), CellData::Int64(8)])
      .unwrap();

    assert_eq!(
      vec![
        Row(&[CellData::Int32(5), CellData::Int64(6)]),
        Row(&[CellData::Int32(7), CellData::Int64(8)]),
      ],
      table_data.rows().collect::<Vec<_>>()
    )
  }
}
