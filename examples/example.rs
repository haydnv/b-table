use std::io;
use std::path::Path;

use async_trait::async_trait;
use b_tree::{Key, Node};
use bytes::Bytes;
use destream::{de, en};
use destream_json::Value;
use freqfs::FileLoad;
use futures::{TryFutureExt, TryStreamExt};
use safecast::as_type;
use tokio::fs;
use tokio_util::io::StreamReader;

const BLOCK_SIZE: usize = 4_096;

enum File {
    Node(Node<Vec<Key<i16>>>),
}

#[async_trait]
impl de::FromStream for File {
    type Context = ();

    async fn from_stream<D: de::Decoder>(cxt: (), decoder: &mut D) -> Result<Self, D::Error> {
        Node::from_stream(cxt, decoder).map_ok(Self::Node).await
    }
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Node(node) => node.to_stream(encoder),
        }
    }
}

as_type!(File, Node, Node<Vec<Key<i16>>>);

#[async_trait]
impl FileLoad for File {
    async fn load(
        _path: &Path,
        file: fs::File,
        _metadata: std::fs::Metadata,
    ) -> Result<Self, io::Error> {
        destream_json::de::read_from((), file)
            .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause))
            .await
    }

    async fn save(&self, file: &mut fs::File) -> Result<u64, io::Error> {
        let encoded = destream_json::en::encode(self)
            .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause))?;

        let mut reader = StreamReader::new(
            encoded
                .map_ok(Bytes::from)
                .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause)),
        );

        let size = tokio::io::copy(&mut reader, file).await?;
        assert!(size > 0);
        Ok(size)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct IndexSchema {
    columns: Vec<String>,
}

impl b_tree::Schema for IndexSchema {
    type Error = io::Error;
    type Value = Value;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
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
