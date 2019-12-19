use std::collections::{BTreeMap, BTreeSet};
use std::iter::Iterator;

use safe_transmute;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::check_schema::{check_row_schema, SchemaError};
use crate::common::{Value, ValueType};
use crate::schema::{BTreeIndexSchema, ColumnSchema, TableSchema};

const SIZE_OF_HEADER: usize = 1;
const PAGE_SIZE: usize = 8192;

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct Row<'a>(&'a [Value]);

impl<'a> Row<'a> {
  pub fn new(data: &'a [Value]) -> Self {
    Row(data)
  }
}

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct MutRow<'a>(&'a mut [Value]);

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord)]
pub struct PageRowRef(pub(crate) u16);

pub struct BTreeIndex<'a> {
  schema: &'a BTreeIndexSchema,
  data: BTreeMap<Value, PageRowRef>,
}

impl<'a> BTreeIndex<'a> {
  pub fn from_schema(schema: &'a BTreeIndexSchema) -> BTreeIndex<'a> {
    BTreeIndex {
      schema,
      data: BTreeMap::new(),
    }
  }

  pub fn lookup(&self, key: Value) -> Option<PageRowRef> {
    self.data.get(&key).cloned()
  }
}

pub struct TablePage<'a> {
  schema: &'a TableSchema,
  data: Vec<Value>,
  header: PageHeader,
  data2: Vec<u8>,
  freed_rows: BTreeSet<PageRowRef>,
  indices: Vec<BTreeIndex<'a>>,
}

pub struct PageHeader {
  start_of_free: usize,
  end_of_free: usize,
}

impl<'a> TablePage<'a> {
  pub fn from_schema(schema: &'a TableSchema) -> TablePage<'a> {
    TablePage {
      schema,
      data: Vec::new(),
      header: PageHeader {
        start_of_free: 0,
        end_of_free: PAGE_SIZE,
      },
      data2: vec![0u8; PAGE_SIZE],
      freed_rows: BTreeSet::new(),
      indices: schema
        .indices()
        .iter()
        .map(BTreeIndex::from_schema)
        .collect(),
    }
  }

  pub fn rows(&self) -> impl Iterator<Item = Row> {
    let freed_rows = &self.freed_rows;

    self
      .data
      .chunks(self.schema.columns().len())
      .enumerate()
      .filter(move |(i, _)| !freed_rows.contains(&PageRowRef(*i as u16)))
      .map(|(_, data)| Row(data))
  }

  pub fn row_numbers(&'a self) -> impl Iterator<Item = PageRowRef> + 'a {
    let freed_rows = &self.freed_rows;

    (0..self.allocated_rows_len() as u16)
      .map(PageRowRef)
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

  fn row_data_offset(&self, i: u16) -> usize {
    i as usize * self.stride()
  }

  fn get_row_range(&self, row_number: PageRowRef) -> std::ops::Range<usize> {
    let offset = self.row_data_offset(row_number.0);
    offset..offset + self.stride()
  }

  pub fn get_row(&self, row_number: PageRowRef) -> Row {
    debug_assert!((row_number.0 as usize) < self.rows_len());
    debug_assert!(!self.freed_rows.contains(&row_number));

    Row(&self.data[self.get_row_range(row_number)])
  }

  fn has_space_for_row_sized(&self, required_space: usize) -> bool {
    true
  }

  fn insert_new_row(&mut self, row_data: Vec<u8>) {
    let row_length = row_data.len();

    let new_row_start = self.header.end_of_free - row_length;
    let new_row_end = self.header.end_of_free;

    let new_row_number_start = self.header.start_of_free;

    (&mut self.data2[new_row_start..new_row_end]).copy_from_slice(&row_data);
    (&mut self.data2[new_row_number_start..])
      .write_u16::<LittleEndian>(new_row_start as u16)
      .unwrap();

    self.header.start_of_free += 2;
    self.header.end_of_free -= row_length;
  }

  fn insert_bytes(&mut self, row_data: Vec<u8>) {
    // TODO reuse this buffer
    let mut offsets = Vec::new();
    self.row_offsets(&mut offsets);

    for offset in offsets.windows(2) {
      let length = offset[1] - offset[0];

      // 1 = size of header
      if length as usize >= row_data.len() + SIZE_OF_HEADER {}
    }
  }

  fn row_offsets(&self, target: &mut Vec<u16>) {
    for i in (0..self.header.start_of_free as usize).step_by(2) {
      let offset = (&self.data2[i..i + 2]).read_u16::<LittleEndian>().unwrap();
      target.push(offset);
    }
  }

  pub fn insert(&mut self, row: Row) -> Result<PageRowRef, SchemaError> {
    check_row_schema(self.schema, &row.0)?;

    let stride = self.stride();

    let row_number = match self.freed_rows.iter().nth(0).cloned() {
      Some(row_number) => {
        let offset = self.row_data_offset(row_number.0);
        self.data.as_mut_slice()[offset..offset + stride].clone_from_slice(row.0);
        self.freed_rows.remove(&row_number);
        row_number
      }
      None => {
        self.data.extend_from_slice(row.0);
        PageRowRef((self.rows_len() - 1) as u16)
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
    row_number: PageRowRef,
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

  pub fn delete(&mut self, row_number: PageRowRef) {
    debug_assert!((row_number.0 as usize) < self.rows_len());
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

    for i in 0..self.allocated_rows_len() as u16 {
      debug_assert!(
        self.freed_rows.insert(PageRowRef(i)),
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
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)]);
    let mut table_data = TablePage::from_schema(&table_schema);

    table_data
      .insert(Row(&[Value::Int32(123)]))
      .expect("Inserting a valid row should succeed.");
  }

  #[test]
  fn insert_null_to_non_nullable_fails() {
    let table_schema = TableSchema::new(
      "test_table",
      &[ColumnSchema::new("not_nullable", ValueType::Int32)],
    );
    let mut table_data = TablePage::from_schema(&table_schema);

    table_data
      .insert(Row(&[Value::Null]))
      .expect_err("Inserting a null into a non-nullable column should fail.");
  }

  #[test]
  fn can_iterate_rows() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)]);

    let mut table_data = TablePage::from_schema(&table_schema);
    table_data.data = vec![Value::Int32(123), Value::Int32(124)];

    assert_eq!(
      vec![Row(&[Value::Int32(123)]), Row(&[Value::Int32(124)]),],
      table_data.rows().collect::<Vec<_>>()
    )
  }

  #[test]
  fn truncate_marks_rows_as_deleted() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)]);

    let mut table_data = TablePage::from_schema(&table_schema);
    table_data.data = vec![Value::Int32(1), Value::Int32(2)];

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
        ColumnSchema::new("a", ValueType::Int32),
        ColumnSchema::new("b", ValueType::Int64),
      ],
    );

    let mut table_data = TablePage::from_schema(&table_schema);

    table_data
      .insert(Row(&[Value::Int32(1), Value::Int64(2)]))
      .unwrap();
    table_data
      .insert(Row(&[Value::Int32(3), Value::Int64(4)]))
      .unwrap();

    table_data.truncate();

    table_data
      .insert(Row(&[Value::Int32(5), Value::Int64(6)]))
      .unwrap();
    table_data
      .insert(Row(&[Value::Int32(7), Value::Int64(8)]))
      .unwrap();

    assert_eq!(
      table_data.rows().collect::<Vec<_>>(),
      vec![
        Row(&[Value::Int32(5), Value::Int64(6)]),
        Row(&[Value::Int32(7), Value::Int64(8)]),
      ],
      "Table should contain expected rows after insertion-truncation-insertion."
    )
  }

  #[test]
  fn delete_marks_row_as_deleted() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)]);

    let mut table_data = TablePage::from_schema(&table_schema);
    table_data.data = vec![Value::Int32(1), Value::Int32(2)];

    table_data.delete(PageRowRef(0));

    assert_eq!(table_data.rows_len(), 1);
    assert_eq!(table_data.allocated_rows_len(), 2);
    assert_eq!(
      table_data.rows().collect::<Vec<_>>(),
      vec![Row(&[Value::Int32(2)])]
    );
  }

  #[test]
  fn insert_insert_delete_insert() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)]);

    let mut table_data = TablePage::from_schema(&table_schema);

    table_data.insert(Row(&[Value::Int32(1)])).unwrap();
    let row_number = table_data.insert(Row(&[Value::Int32(2)])).unwrap();
    table_data.delete(row_number);
    table_data.insert(Row(&[Value::Int32(3)])).unwrap();

    assert_eq!(
      table_data.rows().collect::<Vec<_>>(),
      vec![Row(&[Value::Int32(1)]), Row(&[Value::Int32(3)])]
    );
  }

  #[test]
  fn index_is_updated_on_insert() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TablePage::from_schema(&table_schema);

    table_data.insert(Row(&[Value::Int32(1)])).unwrap();
    table_data.insert(Row(&[Value::Int32(2)])).unwrap();
    table_data.insert(Row(&[Value::Int32(3)])).unwrap();

    assert_eq!(
      table_data.indices[0].lookup(Value::Int32(1)),
      Some(PageRowRef(0))
    );
    assert_eq!(
      table_data.indices[0].lookup(Value::Int32(2)),
      Some(PageRowRef(1))
    );
    assert_eq!(
      table_data.indices[0].lookup(Value::Int32(3)),
      Some(PageRowRef(2))
    );
  }

  #[test]
  fn index_is_updated_on_delete() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TablePage::from_schema(&table_schema);

    table_data.insert(Row(&[Value::Int32(1)])).unwrap();
    let row_number = table_data.insert(Row(&[Value::Int32(2)])).unwrap();
    table_data.insert(Row(&[Value::Int32(3)])).unwrap();

    assert_eq!(
      table_data.indices[0].lookup(Value::Int32(2)),
      Some(row_number)
    );

    table_data.delete(row_number);

    assert_eq!(table_data.indices[0].lookup(Value::Int32(2)), None);
  }

  #[test]
  fn index_is_updated_on_truncate() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TablePage::from_schema(&table_schema);

    table_data.insert(Row(&[Value::Int32(1)])).unwrap();
    table_data.insert(Row(&[Value::Int32(2)])).unwrap();
    table_data.insert(Row(&[Value::Int32(3)])).unwrap();

    table_data.truncate();

    assert_eq!(table_data.indices[0].lookup(Value::Int32(1)), None);
    assert_eq!(table_data.indices[0].lookup(Value::Int32(2)), None);
    assert_eq!(table_data.indices[0].lookup(Value::Int32(3)), None);
  }

  #[test]
  fn can_update_with_valid_value() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TablePage::from_schema(&table_schema);

    let row_number = table_data.insert(Row(&[Value::Int32(1)])).unwrap();
    table_data
      .update(row_number, |row| row.0[0] = Value::Int32(2))
      .expect("Update should succeed.");
  }

  #[test]
  fn index_is_updated_on_update() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TablePage::from_schema(&table_schema);

    let row_number = table_data.insert(Row(&[Value::Int32(1)])).unwrap();

    table_data
      .update(row_number, |row| row.0[0] = Value::Int32(100))
      .unwrap();

    assert_eq!(
      table_data.indices[0].lookup(Value::Int32(100)),
      Some(row_number),
      "New value should exists in index"
    );
    assert_eq!(
      table_data.indices[0].lookup(Value::Int32(1)),
      None,
      "Old value should not exist in index"
    );
  }

  #[test]
  fn schema_is_validated_after_update() {
    let table_schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)])
      .with_index(BTreeIndexSchema::new(0));
    let mut table_data = TablePage::from_schema(&table_schema);

    let row_number = table_data.insert(Row(&[Value::Int32(1)])).unwrap();

    table_data
      .update(row_number, |row| row.0[0] = Value::Int64(101))
      .expect_err("Changing column type in update should result in an error.");
  }
}
