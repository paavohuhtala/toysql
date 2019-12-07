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

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct MutRow<'a>(&'a mut [CellData]);

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord)]
pub struct RowNumber(usize);

impl Into<usize> for RowNumber {
  fn into(self) -> usize {
    self.0
  }
}

pub struct BTreeIndex<'a> {
  schema: &'a BTreeIndexSchema,
  data: BTreeMap<CellData, RowNumber>,
}

impl<'a> BTreeIndex<'a> {
  pub fn from_schema(schema: &'a BTreeIndexSchema) -> BTreeIndex<'a> {
    BTreeIndex {
      schema,
      data: BTreeMap::new(),
    }
  }

  pub fn lookup(&self, key: CellData) -> Option<RowNumber> {
    self.data.get(&key).cloned()
  }
}

pub struct TableData<'a> {
  schema: &'a TableSchema,
  data: Vec<CellData>,
  freed_rows: BTreeSet<RowNumber>,
  indices: Vec<BTreeIndex<'a>>,
}

impl<'a> TableData<'a> {
  pub fn from_schema(schema: &'a TableSchema) -> TableData<'a> {
    TableData {
      schema,
      data: Vec::new(),
      freed_rows: BTreeSet::new(),
      indices: schema
        .indices()
        .iter()
        .map(BTreeIndex::from_schema)
        .collect(),
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

  fn row_data_offset(&self, i: impl Into<usize>) -> usize {
    i.into() * self.stride()
  }

  fn get_row_range(&self, row_number: RowNumber) -> std::ops::Range<usize> {
    let offset = self.row_data_offset(row_number);
    offset..offset + self.stride()
  }

  pub fn get_row(&self, row_number: RowNumber) -> Row {
    debug_assert!(row_number.0 < self.rows_len());
    debug_assert!(!self.freed_rows.contains(&row_number));

    Row(&self.data[self.get_row_range(row_number)])
  }

  pub fn insert(&mut self, row: Row) -> Result<RowNumber, SchemaError> {
    check_row_schema(self.schema, &row.0)?;

    let stride = self.stride();

    let row_number = match self.freed_rows.iter().nth(0).cloned() {
      Some(row_number) => {
        let offset = self.row_data_offset(row_number);
        self.data.as_mut_slice()[offset..offset + stride].clone_from_slice(row.0);
        self.freed_rows.remove(&row_number);
        row_number
      }
      None => {
        self.data.extend_from_slice(row.0);
        RowNumber(self.rows_len() - 1)
      }
    };

    for index in &mut self.indices {
      let key = row.0[index.schema.column_index].clone();
      index.data.insert(key, row_number);
    }

    Ok(row_number)
  }

  pub fn update(
    &mut self,
    row_number: RowNumber,
    update_fn: impl Fn(MutRow),
  ) -> Result<(), SchemaError> {
    let row_before = self.get_row(row_number);
    let row_range = self.get_row_range(row_number);

    let old_keys = self
      .indices
      .iter()
      .map(|index| row_before.0[index.schema.column_index].clone())
      .collect::<Vec<_>>();

    let row_ref = &mut self.data[row_range];
    update_fn(MutRow(row_ref));

    check_row_schema(self.schema, row_ref)?;

    let updated_row = &*row_ref;

    for (index, old_key) in self.indices.iter_mut().zip(old_keys.iter()) {
      index.data.remove(old_key);
      let key = updated_row[index.schema.column_index].clone();
      index.data.insert(key, row_number);
    }

    Ok(())
  }

  pub fn delete(&mut self, row_number: RowNumber) {
    debug_assert!(row_number.0 < self.rows_len());
    debug_assert!(!self.freed_rows.contains(&row_number));

    let row = Row(&self.data[self.get_row_range(row_number)]);

    for index in &mut self.indices {
      let key = &row.0[index.schema.column_index];
      index.data.remove(key);
    }

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
  fn insert_insert_delete_insert() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)]);

    let mut table_data = TableData::from_schema(&table_schema);

    table_data.insert(Row(&[CellData::Int32(1)])).unwrap();
    let row_number = table_data.insert(Row(&[CellData::Int32(2)])).unwrap();
    table_data.delete(row_number);
    table_data.insert(Row(&[CellData::Int32(3)])).unwrap();

    assert_eq!(
      table_data.rows().collect::<Vec<_>>(),
      vec![Row(&[CellData::Int32(1)]), Row(&[CellData::Int32(3)])]
    );
  }

  #[test]
  fn index_is_updated_on_insert() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TableData::from_schema(&table_schema);

    table_data.insert(Row(&[CellData::Int32(1)])).unwrap();
    table_data.insert(Row(&[CellData::Int32(2)])).unwrap();
    table_data.insert(Row(&[CellData::Int32(3)])).unwrap();

    assert_eq!(
      table_data.indices[0].lookup(CellData::Int32(1)),
      Some(RowNumber(0))
    );
    assert_eq!(
      table_data.indices[0].lookup(CellData::Int32(2)),
      Some(RowNumber(1))
    );
    assert_eq!(
      table_data.indices[0].lookup(CellData::Int32(3)),
      Some(RowNumber(2))
    );
  }

  #[test]
  fn index_is_updated_on_delete() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TableData::from_schema(&table_schema);

    table_data.insert(Row(&[CellData::Int32(1)])).unwrap();
    let row_number = table_data.insert(Row(&[CellData::Int32(2)])).unwrap();
    table_data.insert(Row(&[CellData::Int32(3)])).unwrap();

    assert_eq!(
      table_data.indices[0].lookup(CellData::Int32(2)),
      Some(row_number)
    );

    table_data.delete(row_number);

    assert_eq!(table_data.indices[0].lookup(CellData::Int32(2)), None);
  }

  #[test]
  fn index_is_updated_on_truncate() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TableData::from_schema(&table_schema);

    table_data.insert(Row(&[CellData::Int32(1)])).unwrap();
    table_data.insert(Row(&[CellData::Int32(2)])).unwrap();
    table_data.insert(Row(&[CellData::Int32(3)])).unwrap();

    table_data.truncate();

    assert_eq!(table_data.indices[0].lookup(CellData::Int32(1)), None);
    assert_eq!(table_data.indices[0].lookup(CellData::Int32(2)), None);
    assert_eq!(table_data.indices[0].lookup(CellData::Int32(3)), None);
  }

  #[test]
  fn can_update_with_valid_value() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TableData::from_schema(&table_schema);

    let row_number = table_data.insert(Row(&[CellData::Int32(1)])).unwrap();
    table_data
      .update(row_number, |row| row.0[0] = CellData::Int32(2))
      .expect("Update should succeed.");
  }

  #[test]
  fn index_is_updated_on_update() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TableData::from_schema(&table_schema);

    let row_number = table_data.insert(Row(&[CellData::Int32(1)])).unwrap();

    table_data
      .update(row_number, |row| row.0[0] = CellData::Int32(100))
      .unwrap();

    assert_eq!(
      table_data.indices[0].lookup(CellData::Int32(100)),
      Some(row_number),
      "New value should exists in index"
    );
    assert_eq!(
      table_data.indices[0].lookup(CellData::Int32(1)),
      None,
      "Old value should not exist in index"
    );
  }

  #[test]
  fn schema_is_validated_after_update() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ColumnType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TableData::from_schema(&table_schema);

    let row_number = table_data.insert(Row(&[CellData::Int32(1)])).unwrap();

    table_data
      .update(row_number, |row| row.0[0] = CellData::Int64(101))
      .expect_err("Changing column type in update should result in an error.");
  }
}
