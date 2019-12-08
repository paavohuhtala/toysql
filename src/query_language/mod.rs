use pest::iterators::Pair;
use pest::Parser;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Parser)]
#[grammar = "./query_language/query.pest"]
pub struct QueryParser;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
  Integer(i128),
}

pub type Record = HashMap<String, Expression>;

#[derive(Debug, PartialEq, Eq)]
pub enum Statement {
  Insert { table: String, row: Record },
}

fn parse_table_or_column_name(pair: Pair<Rule>) -> String {
  assert_eq!(pair.as_rule(), Rule::table_or_column_name);
  pair.as_str().to_string()
}

fn parse_expression(pair: Pair<Rule>) -> Expression {
  assert_eq!(pair.as_rule(), Rule::literal);
  Expression::Integer(i128::from_str(pair.into_inner().next().unwrap().as_str()).unwrap())
}

fn parse_record(pair: Pair<Rule>) -> HashMap<String, Expression> {
  assert_eq!(pair.as_rule(), Rule::record);
  pair
    .into_inner()
    .map(|pair| {
      assert_eq!(pair.as_rule(), Rule::key_value);
      let mut pair = pair.into_inner();
      let key = parse_table_or_column_name(pair.next().unwrap());
      let value = parse_expression(pair.next().unwrap());
      (key, value)
    })
    .collect()
}

impl Statement {
  pub fn parse(s: &str) -> Statement {
    let mut tokens = QueryParser::parse(Rule::statement, s).unwrap();
    Statement::from_pair(tokens.next().unwrap())
  }

  fn from_pair(pair: Pair<Rule>) -> Statement {
    assert_eq!(Rule::statement, pair.as_rule());
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
      Rule::insert_statement => {
        let mut inner_rules = inner.into_inner();
        let table = parse_table_or_column_name(inner_rules.next().unwrap());
        let row = parse_record(inner_rules.next().unwrap());
        Statement::Insert { table, row }
      }
      otherwise => panic!("Unexpected rule: {:?}", otherwise),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn insert_record() {
    let statement = Statement::parse("(insert test_table { x: 10, y: 20 })");

    let mut expected_record = HashMap::new();
    expected_record.insert("x".to_string(), Expression::Integer(10));
    expected_record.insert("y".to_string(), Expression::Integer(20));

    assert_eq!(
      statement,
      Statement::Insert {
        table: "test_table".to_string(),
        row: expected_record
      }
    );
  }
}
