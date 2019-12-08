use crate::data::CellData;
use crate::schema::{ColumnSchema, ColumnType, TableSchema};

#[derive(Debug)]
pub enum SchemaError {
  InvalidColumn {
    index: usize,
    value: CellData,
    column: String,
    table: String,
  },
  InvalidRow {
    length: usize,
    table: String,
  },
}

pub fn check_row_schema(table: &TableSchema, row: &[CellData]) -> Result<(), SchemaError> {
  if row.len() != table.columns().len() {
    return Err(SchemaError::InvalidRow {
      length: row.len(),
      table: table.name().to_string(),
    });
  }

  for (i, (cell, column)) in row.iter().zip(table.columns().iter()).enumerate() {
    let is_valid = match cell.type_of() {
      ColumnType::Null if !column.nullable => false,
      cell_type if column.column_type != cell_type => false,
      _ => true,
    };

    if !is_valid {
      return Err(SchemaError::InvalidColumn {
        index: i,
        value: cell.clone(),
        column: column.name.clone(),
        table: table.name().to_string(),
      });
    }
  }

  Ok(())
}
