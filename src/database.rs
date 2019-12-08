use crate::check_schema::SchemaError;
use crate::data::{CellData, Row, RowNumber, TableData};
use crate::query_language::{Expression, Record, Statement};
use crate::schema::{ColumnSchema, ColumnType, DatabaseSchema, TableSchema};
use std::collections::HashMap;

pub struct Table<'a> {
  schema: &'a TableSchema,
  pages: Vec<TableData<'a>>,
}

impl<'a> Table<'a> {
  pub fn new(schema: &'a TableSchema) -> Self {
    Table {
      schema,
      pages: vec![TableData::from_schema(schema)],
    }
  }

  pub fn rows(&'a self) -> impl Iterator<Item = Row> + 'a {
    let iter = self.pages.iter();
    iter.flat_map(|page| page.rows())
  }

  pub fn insert(&mut self, row: Row) -> Result<RowNumber, SchemaError> {
    self.pages[0].insert(row)
  }

  pub fn insert_record<'b>(&'b mut self, record: Record) -> Result<RowNumber, QueryError> {
    let row = self.record_to_row(record)?;
    self.insert(Row::new(&row)).map_err(|err| err.into())
  }

  fn expression_to_cell(column: &'a ColumnSchema, expression: &Expression) -> Option<CellData> {
    match (column.column_type, expression) {
      (ColumnType::Int32, Expression::Integer(i)) => Some(CellData::Int32(*i as i32)),
      (ColumnType::Int64, Expression::Integer(i)) => Some(CellData::Int64(*i as i64)),
      _ => None,
    }
  }

  fn record_to_row(&self, record: Record) -> Result<Vec<CellData>, QueryError> {
    self
      .schema
      .columns()
      .iter()
      .map(|column| match record.get(&column.name) {
        Some(expr) => match Self::expression_to_cell(column, expr) {
          Some(cell) => Ok(cell),
          None => Err(QueryError::InvalidExpressionForColumn {
            table: self.schema.name().to_string(),
            column: column.name.clone(),
            expected: column.column_type,
            expression: (*expr).clone(),
          }),
        },
        None => Err(QueryError::MissingColumn {
          table: self.schema.name().to_string(),
          column: column.name.clone(),
        }),
      })
      .collect()
  }
}

pub struct Database<'a> {
  schema: &'a DatabaseSchema,
  tables: HashMap<String, Table<'a>>,
}

#[derive(Debug)]
pub enum QueryError {
  InvalidQuery,
  UnknownTable {
    table: String,
  },
  MissingColumn {
    table: String,
    column: String,
  },
  InvalidExpressionForColumn {
    table: String,
    column: String,
    expected: ColumnType,
    expression: Expression,
  },
  SchemaError(SchemaError),
}

impl From<SchemaError> for QueryError {
  fn from(err: SchemaError) -> Self {
    QueryError::SchemaError(err)
  }
}

impl<'a> Database<'a> {
  pub fn new(schema: &'a DatabaseSchema) -> Database {
    Database {
      schema,
      tables: schema
        .tables()
        .iter()
        .map(|table| (table.name().to_string(), Table::new(table)))
        .collect(),
    }
  }

  pub fn execute_statement(&mut self, statement: &str) -> Result<(), QueryError> {
    match Statement::parse(statement) {
      Statement::Insert { table, row } => {
        let maybe_table = self.tables.get_mut(&table);

        match maybe_table {
          None => return Err(QueryError::UnknownTable { table }),
          Some(table) => {
            table.insert_record(row)?;
          }
        }
      }
    }

    Ok(())
  }

  pub fn get_table(&self, name: &str) -> Option<&Table> {
    self.tables.get(name)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_insert() {
    let schema = DatabaseSchema::new(&[TableSchema::new(
      "my_cool_table",
      &[ColumnSchema::new("cool_column", ColumnType::Int64)],
    )]);

    let mut database = Database::new(&schema);

    database
      .execute_statement("(insert my_cool_table { cool_column: 12345 })")
      .expect("Insertion should succeed.");

    database
      .execute_statement("(insert my_cool_table { cool_column: 12346 })")
      .expect("Insertion should succeed.");

    assert_eq!(
      database
        .get_table("my_cool_table")
        .unwrap()
        .rows()
        .collect::<Vec<_>>(),
      vec![
        Row::new(&[CellData::Int64(12345)]),
        Row::new(&[CellData::Int64(12346)])
      ]
    );
  }
}
