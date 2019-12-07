use crate::data::CellData;
use crate::schema::{ColumnSchema, ColumnType, TableSchema};

#[derive(Debug)]
pub enum SchemaError<'a> {
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

pub fn check_row_schema<'a>(
  table: &'a TableSchema,
  row: &[CellData],
) -> Result<(), SchemaError<'a>> {
  if row.len() != table.columns().len() {
    return Err(SchemaError::InvalidRow {
      length: row.len(),
      table,
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
        column,
        table,
      });
    }
  }

  Ok(())
}
