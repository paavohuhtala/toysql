use std::collections::{BTreeMap, BTreeSet};
use std::iter::Iterator;

use safe_transmute;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::check_schema::{check_row_schema, SchemaError};
use crate::common::{Value, ValueType};
use crate::row::{PackedRow, RowWriter};
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
  freed_rows: BTreeSet<PageRowRef>,
  indices: Vec<BTreeIndex<'a>>,
}

pub struct TablePage2<'a> {
  schema: &'a TableSchema,
  header: PageHeader,
  data: Vec<u8>,
}

pub struct PageHeader {
  start_of_free: usize,
  end_of_free: usize,
}

impl<'a> TablePage2<'a> {
  pub fn from_schema(schema: &'a TableSchema) -> TablePage2<'a> {
    TablePage2 {
      schema,
      header: PageHeader {
        start_of_free: 0,
        end_of_free: PAGE_SIZE,
      },
      data: vec![0u8; PAGE_SIZE],
    }
  }

  fn has_space_for_row_sized(&self, required_space: usize) -> bool {
    true
  }

  fn insert_new_row(&mut self, row_data: Vec<u8>) {
    let row_length = row_data.len();

    let new_row_start = self.header.end_of_free - row_length;
    let new_row_end = self.header.end_of_free;

    let new_row_number_start = self.header.start_of_free;

    (&mut self.data[new_row_start..new_row_end]).copy_from_slice(&row_data);

    let mut new_row_numbers_slice = &mut self.data[new_row_number_start..];

    // Row offset
    new_row_numbers_slice
      .write_u16::<LittleEndian>(new_row_start as u16)
      .unwrap();
    // Row size
    new_row_numbers_slice
      .write_u16::<LittleEndian>(row_data.len() as u16)
      .unwrap();

    self.header.start_of_free += 4;
    self.header.end_of_free -= row_length;
  }

  fn insert_bytes(&mut self, row_data: Vec<u8>) {
    for row in self.iter_rows() {
      if row.is_in_use() {
        continue;
      }

      // First to fit
      // 1 = size of header
      if row.allocated_size() >= row_data.len() + SIZE_OF_HEADER {
        todo!();
      }
    }

    self.insert_new_row(row_data);
  }

  fn iter_rows(&mut self) -> impl Iterator<Item = PackedRow> {
    let (offsets, rows) = self.data.split_at_mut(self.header.end_of_free);

    RowIterator {
      entry_offset: 0,
      remaining_entries: self.header.start_of_free / 4,
      data: &mut self.data,
    }
  }
}

#[derive(Debug)]
struct RowIterator<'a> {
  entry_offset: usize,
  remaining_entries: usize,
  data: &'a mut [u8],
}

impl<'a> Iterator for RowIterator<'a> {
  type Item = PackedRow<'a>;

  fn next(&mut self) -> Option<PackedRow<'a>> {
    if self.remaining_entries == 0 {
      return None;
    }

    let row_offset = LittleEndian::read_u16(&self.data[self.entry_offset..]) as usize;
    let row_length = LittleEndian::read_u16(&self.data[self.entry_offset + 2..]) as usize;
    self.entry_offset += 4;
    self.remaining_entries -= 1;

    assert!(
      row_offset + row_length <= self.data.len(),
      "Expected {} + {} <= {}",
      row_offset,
      row_length,
      self.data.len()
    );

    let ptr = self.data.as_mut_ptr();
    // For this to be safe, there must be NO duplicate row offsets.
    let row = unsafe { std::slice::from_raw_parts_mut(ptr.add(row_offset), row_length) };

    return Some(PackedRow::new(row));
  }
}

impl<'a> TablePage<'a> {
  pub fn from_schema(schema: &'a TableSchema) -> TablePage<'a> {
    TablePage {
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

#[cfg(test)]
mod page_tests {
  use super::*;

  #[test]
  fn new_rows_are_iterable() {
    let schema = TableSchema::new("test_table", &[ColumnSchema::new("a", ValueType::Int32)]);
    let mut page = TablePage2::from_schema(&schema);

    let mut new_row = Vec::new();
    let mut writer = RowWriter::new(&mut new_row, &schema);
    writer.write_header().unwrap();
    writer.write(&1337).unwrap();
    page.insert_bytes(new_row);

    let mut new_row = Vec::new();
    let mut writer = RowWriter::new(&mut new_row, &schema);
    writer.write_header().unwrap();
    writer.write(&666).unwrap();
    page.insert_bytes(new_row);

    assert_eq!(2, page.iter_rows().count());
  }
}
