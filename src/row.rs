use crate::common::{SqlNull, Value};
use crate::schema::TableSchema;
use integer_encoding::*;
use std::io;
use std::io::{Cursor, Read, Write};

pub struct RowWriter<'a, 'b> {
  target: &'a mut Vec<u8>,
  schema: &'b TableSchema,
}

#[derive(Debug)]
#[repr(transparent)]
pub struct PackedRow<'a>(&'a mut [u8]);

impl<'a> PackedRow<'a> {
  pub fn new(slice: &'a mut [u8]) -> PackedRow {
    PackedRow(slice)
  }

  pub fn allocated_size(&self) -> usize {
    self.0.len()
  }

  pub fn is_in_use(&self) -> bool {
    self.0[0] == 1
  }

  pub fn mark_as_deleted(&mut self) {
    self.0[0] = 0;
  }
}

impl<'a, 'b> RowWriter<'a, 'b> {
  pub fn new(target: &'a mut Vec<u8>, schema: &'b TableSchema) -> Self {
    RowWriter { target, schema }
  }

  pub fn write_header(&mut self) -> io::Result<()> {
    let mut header_bytes = [0u8; 4];
    header_bytes[0] = 1;
    self.target.write_all(&header_bytes)
  }

  pub fn write<T: RowWritable>(&mut self, item: &T) -> io::Result<()> {
    item.write_to_row(&mut self.target)
  }
}

pub trait RowWritable: Sized {
  fn write_to_row(&self, writer: &mut impl Write) -> io::Result<()>;
}

pub trait RowReadable: Sized {
  fn read_from_row(reader: &mut impl Read) -> io::Result<Self>;
}

pub trait RowSerializable: RowWritable + RowReadable {}

impl RowWritable for SqlNull {
  fn write_to_row(&self, writer: &mut impl Write) -> io::Result<()> {
    Ok(())
  }
}

impl RowReadable for SqlNull {
  fn read_from_row(reader: &mut impl Read) -> io::Result<Self> {
    Ok((SqlNull))
  }
}

impl RowWritable for i32 {
  fn write_to_row(&self, writer: &mut impl Write) -> io::Result<()> {
    let bytes = self.to_le_bytes();
    writer.write_all(&bytes)
  }
}

impl RowReadable for i32 {
  fn read_from_row(reader: &mut impl Read) -> io::Result<Self> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(i32::from_le_bytes(bytes))
  }
}

impl RowWritable for i64 {
  fn write_to_row(&self, writer: &mut impl Write) -> io::Result<()> {
    let bytes = self.to_le_bytes();
    writer.write_all(&bytes)
  }
}

impl RowReadable for i64 {
  fn read_from_row(reader: &mut impl Read) -> io::Result<Self> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(i64::from_le_bytes(bytes))
  }
}

impl RowWritable for Value {
  fn write_to_row(&self, writer: &mut impl Write) -> io::Result<()> {
    match self {
      Value::Null => SqlNull.write_to_row(writer),
      Value::Int32(i) => i.write_to_row(writer),
      Value::Int64(i) => i.write_to_row(writer),
    }
  }
}
