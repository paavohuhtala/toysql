use std::collections::{BTreeMap, BTreeSet};
use std::iter::Iterator;

use crate::check_schema::{check_row_schema, SchemaError};
use crate::schema::{BTreeIndexSchema, ColumnSchema, ColumnType, TableSchema};

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub enum CellData {
  Null,
  Int32(i32),
  Int64(i64),
}

impl CellData {
  pub fn type_of(&self) -> ColumnType {
    match self {
      CellData::Null => ColumnType::Null,
      CellData::Int32(_) => ColumnType::Int32,
      CellData::Int64(_) => ColumnType::Int64,
    }
  }
}

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct Row<'a>(&'a [CellData]);

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord)]
pub struct RowNumber(usize);

pub struct BTreeIndex<'a> {
  schema: &'a BTreeIndexSchema,
  data: BTreeMap<CellData, RowNumber>,
}

pub struct TableData<'a> {
  schema: &'a TableSchema,
  data: Vec<CellData>,
  freed_rows: BTreeSet<RowNumber>,
  indices: Vec<BTreeIndex<'a>>,
}

impl<'a> TableData<'a> {
  fn from_schema(schema: &'a TableSchema) -> TableData<'a> {
    TableData {
      schema,
      data: Vec::new(),
      freed_rows: BTreeSet::new(),
      indices: Vec::new(),
    }
  }
}

impl<'a> TableData<'a> {
  pub fn rows(&self) -> impl Iterator<Item = Row> {
    let freed_rows = &self.freed_rows;

    self
      .data
      .chunks(self.schema.columns().len())
      .enumerate()
      .filter(move |(i, _)| !freed_rows.contains(&RowNumber(*i)))
      .map(|(_, data)| Row(data))
  }

  pub fn row_numbers(&'a self) -> impl Iterator<Item = RowNumber> + 'a {
    let freed_rows = &self.freed_rows;

    (0..self.allocated_rows_len())
      .map(RowNumber)
      .filter(move |row| !freed_rows.contains(row))
  }

  fn stride(&self) -> usize {
    self.schema.columns().len()
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

  pub fn insert(&mut self, row: Row) -> Result<(), SchemaError> {
    check_row_schema(self.schema, &row.0)?;

    let stride = self.stride();

    match self.freed_rows.iter().nth(0).cloned() {
      Some(row_number) => {
        let offset = self.row_data_offset(row_number);
        self.data.as_mut_slice()[offset..offset + stride].clone_from_slice(row.0);
        self.freed_rows.remove(&row_number);
      }
      None => self.data.extend_from_slice(row.0),
    }

    Ok(())
  }

  pub fn delete(&mut self, row_number: RowNumber) {
    debug_assert!(row_number.0 < self.rows_len());
    debug_assert!(!self.freed_rows.contains(&row_number));
    self.freed_rows.insert(row_number);
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
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)]);
    let mut table_data = TableData::from_schema(&table_schema);

    table_data
      .insert(Row(&[CellData::Int32(123)]))
      .expect("Inserting a valid row should succeed.");
  }

  #[test]
  fn insert_null_to_non_nullable_fails() {
    let table_schema = TableSchema::new(
      "test_table",
      &[ColumnSchema::new("not_nullable", ColumnType::Int32)],
    );
    let mut table_data = TableData::from_schema(&table_schema);

    table_data
      .insert(Row(&[CellData::Null]))
      .expect_err("Inserting a null into a non-nullable column should fail.");
  }

  #[test]
  fn can_iterate_rows() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)]);

    let mut table_data = TableData::from_schema(&table_schema);
    table_data.data = vec![CellData::Int32(123), CellData::Int32(124)];

    assert_eq!(
      vec![Row(&[CellData::Int32(123)]), Row(&[CellData::Int32(124)]),],
      table_data.rows().collect::<Vec<_>>()
    )
  }

  #[test]
  fn truncate_marks_rows_as_deleted() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)]);

    let mut table_data = TableData::from_schema(&table_schema);
    table_data.data = vec![CellData::Int32(1), CellData::Int32(2)];

    table_data.truncate();

    assert_eq!(table_data.rows_len(), 0);
    assert_eq!(table_data.allocated_rows_len(), 2);
    assert_eq!(table_data.rows().count(), 0);
  }

  #[test]
  fn insert_truncate_insert() {
    let table_schema = TableSchema::new(
      "test_table",
      &[
        ColumnSchema::new("a", ColumnType::Int32),
        ColumnSchema::new("b", ColumnType::Int64),
      ],
    );

    let mut table_data = TableData {
      schema: &table_schema,
      data: vec![],
      indices: vec![],
      freed_rows: BTreeSet::new(),
    };

    table_data
      .insert(Row(&[CellData::Int32(1), CellData::Int64(2)]))
      .unwrap();
    table_data
      .insert(Row(&[CellData::Int32(3), CellData::Int64(4)]))
      .unwrap();

    table_data.truncate();

    table_data
      .insert(Row(&[CellData::Int32(5), CellData::Int64(6)]))
      .unwrap();
    table_data
      .insert(Row(&[CellData::Int32(7), CellData::Int64(8)]))
      .unwrap();

    assert_eq!(
      table_data.rows().collect::<Vec<_>>(),
      vec![
        Row(&[CellData::Int32(5), CellData::Int64(6)]),
        Row(&[CellData::Int32(7), CellData::Int64(8)]),
      ],
      "Table should contain expected rows after insertion-truncation-insertion."
    )
  }

  #[test]
  fn delete_marks_row_as_deleted() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)]);

    let mut table_data = TableData::from_schema(&table_schema);
    table_data.data = vec![CellData::Int32(1), CellData::Int32(2)];

    table_data.delete(RowNumber(0));

    assert_eq!(table_data.rows_len(), 1);
    assert_eq!(table_data.allocated_rows_len(), 2);
    assert_eq!(
      table_data.rows().collect::<Vec<_>>(),
      vec![Row(&[CellData::Int32(2)])]
    );
  }

  #[test]
  fn insert_delete_insert() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)]);

    let mut table_data = TableData::from_schema(&table_schema);

    table_data.insert(Row(&[CellData::Int32(1)])).unwrap();
    table_data.insert(Row(&[CellData::Int32(2)])).unwrap();
    table_data.delete(RowNumber(1));
    table_data.insert(Row(&[CellData::Int32(3)])).unwrap();

    assert_eq!(
      table_data.rows().collect::<Vec<_>>(),
      vec![Row(&[CellData::Int32(1)]), Row(&[CellData::Int32(3)])]
    );
  }
}
