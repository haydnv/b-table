use std::io;

use b_tree::Key;
use destream_json::Value;

#[derive(Debug, Eq, PartialEq)]
struct IndexSchema {
    columns: Vec<String>,
}

impl b_tree::Schema for IndexSchema {
    type Error = io::Error;
    type Value = Value;

    fn block_size(&self) -> usize {
        4_096
    }

    fn extract_key(&self, key: &[Self::Value], other: &Self) -> Key<Self::Value> {
        assert_eq!(key.len(), self.len());

        let mut other_key = Vec::with_capacity(other.len());
        for i in 0..other.len() {
            let column = &other.columns[i];
            for j in 0..self.len() {
                if column == &self.columns[j] {
                    other_key.push(key[j].clone());
                }
            }
        }

        debug_assert_eq!(other_key.len(), other.len());

        other_key
    }

    fn len(&self) -> usize {
        4
    }

    fn order(&self) -> usize {
        8
    }

    fn validate(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
        if key.len() == self.len() {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrong number of values",
            ))
        }
    }
}

struct Schema {
    primary: IndexSchema,
    auxiliary: Vec<IndexSchema>,
}

impl b_table::Schema for Schema {
    type Id = &'static str;
    type Error = io::Error;
    type Value = Value;
    type Index = IndexSchema;

    fn primary(&self) -> &Self::Index {
        &self.primary
    }

    fn auxiliary(&self) -> &[Self::Index] {
        &self.auxiliary
    }

    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
        if key.len() == 1 {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid key: {:?}", key),
            ))
        }
    }

    fn validate_values(&self, values: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
        if values.len() == 3 {
            Ok(values)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid values: {:?}", values),
            ))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    todo!()
}
