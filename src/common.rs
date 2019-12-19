#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ValueType {
  UnknownNull,
  Int32,
  Int64,
}

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub enum Value {
  Null,
  Int32(i32),
  Int64(i64),
}

impl Value {
  pub fn type_of(&self) -> ValueType {
    match self {
      Value::Null => ValueType::UnknownNull,
      Value::Int32(_) => ValueType::Int32,
      Value::Int64(_) => ValueType::Int64,
    }
  }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ColumnType {
  pub base_type: ValueType,
  pub is_nullable: bool,
}

#[repr(u8)]
pub enum RowItemTag {
  Null = 0,
  Int32 = 1,
  Int64 = 2,
}

pub struct SqlNull;
