#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ValueType {
  Null,
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
      Value::Null => ValueType::Null,
      Value::Int32(_) => ValueType::Int32,
      Value::Int64(_) => ValueType::Int64,
    }
  }
}
