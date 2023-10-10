use std::cmp::Ordering;
use std::io;
use std::ops::Bound;
use std::path::PathBuf;

use b_table::collate::{self, Collate};
use b_table::{BTreeSchema, IndexSchema as IndexSchemaInstance, Node, Range, TableLock};
use destream::en;
use destream_json::Value;
use freqfs::Cache;
use futures::TryStreamExt;
use number_general::NumberCollator;
use rand::Rng;
use safecast::as_type;
use smallvec::smallvec;
use tokio::fs;

const BLOCK_SIZE: usize = 4_096;

#[derive(Copy, Clone, Eq, PartialEq)]
struct Collator {
    string: collate::Collator<String>,
    number: NumberCollator,
}

impl Collator {
    fn new() -> Self {
        Self {
            string: collate::Collator::default(),
            number: NumberCollator::default(),
        }
    }
}

impl Collate for Collator {
    type Value = Value;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        match (left, right) {
            (Value::String(l), Value::String(r)) => self.string.cmp(l, r),
            (Value::Number(l), Value::Number(r)) => self.number.cmp(l, r),
            (l, r) => panic!("tried to compare un-like types: {:?} vs {:?}", l, r),
        }
    }
}

enum File {
    Node(Node<Value>),
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Node(node) => node.to_stream(encoder),
        }
    }
}

as_type!(File, Node, Node<Value>);

#[derive(Clone, Debug, Eq, PartialEq)]
struct IndexSchema {
    columns: Vec<String>,
}

impl IndexSchema {
    fn new<C: IntoIterator<Item = &'static str>>(columns: C) -> Self {
        Self {
            columns: columns.into_iter().map(String::from).collect(),
        }
    }
}

impl BTreeSchema for IndexSchema {
    type Error = io::Error;
    type Value = Value;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn len(&self) -> usize {
        self.columns.len()
    }

    fn order(&self) -> usize {
        8
    }

    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
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

impl IndexSchemaInstance for IndexSchema {
    type Id = String;

    fn columns(&self) -> &[Self::Id] {
        &self.columns
    }
}

#[derive(Debug, Eq, PartialEq)]
struct TableSchema {
    primary: IndexSchema,
    auxiliary: Vec<(String, IndexSchema)>,
}

impl TableSchema {
    fn new<C, I>(columns: C, indices: I) -> Self
    where
        C: IntoIterator<Item = &'static str>,
        I: IntoIterator<Item = (String, C)>,
    {
        Self {
            primary: IndexSchema::new(columns),
            auxiliary: indices
                .into_iter()
                .map(|(name, columns)| (name, IndexSchema::new(columns)))
                .collect(),
        }
    }
}

impl b_table::Schema for TableSchema {
    type Id = String;
    type Error = io::Error;
    type Value = Value;
    type Index = IndexSchema;

    fn key(&self) -> &[Self::Id] {
        &self.primary.columns[..1]
    }

    fn values(&self) -> &[Self::Id] {
        &self.primary.columns[1..]
    }

    fn primary(&self) -> &Self::Index {
        &self.primary
    }

    fn auxiliary(&self) -> &[(String, IndexSchema)] {
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

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut rng = rand::thread_rng();
    loop {
        let rand: u32 = rng.gen();
        let path = PathBuf::from(format!("/tmp/test_table_{}", rand));
        if !path.exists() {
            fs::create_dir(&path).await?;
            break Ok(path);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    // set up the test directory
    let path = setup_tmp_dir().await?;

    // initialize the cache
    let cache = Cache::<File>::new(BLOCK_SIZE, None);

    // load the directory and file paths into memory (not file contents, yet)
    let dir = cache.load(path.clone())?;

    // construct the schema
    let schema = TableSchema::new(
        vec!["up", "up_name", "down", "down_name"],
        [
            ("up_name".into(), vec!["up_name", "up"]),
            ("down".into(), vec!["down", "up"]),
        ],
    );

    let row1 = vec![
        1.into(),
        "one".to_string().into(),
        9.into(),
        "nine".to_string().into(),
    ];

    let row2 = vec![
        2.into(),
        "two".to_string().into(),
        8.into(),
        "eight".to_string().into(),
    ];

    // create the table
    let table = TableLock::create(schema, Collator::new(), dir)?;

    // test reading from an empty table
    {
        let guard = table.read().await;
        let range = Range::default();
        assert_eq!(guard.count(range.clone()).await?, 0);

        assert!(guard.is_empty(range).await?);

        let range = Range::from_iter([("up".to_string(), Value::Number(1.into()))]);
        assert_eq!(guard.count(range.clone()).await?, 0);

        assert!(guard.is_empty(range).await?);
    }

    {
        // test inserting a row
        {
            let mut guard = table.write().await;

            let key = row1[..1].to_vec();
            let values = row1[1..].to_vec();

            assert!(guard.upsert(key.clone(), values.clone()).await?);
            assert!(!guard.upsert(key, values).await?);
        }

        // test reading a row
        {
            let guard = table.read().await;
            assert_eq!(guard.count(Default::default()).await?, 1);
            assert!(!guard.is_empty(Default::default()).await?);

            let range = Range::from_iter([("up".to_string(), Value::Number(1.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 1);

            assert!(!guard.is_empty(range).await?);

            let range =
                Range::from_iter([("up_name".to_string(), Value::String("one".to_string()))]);

            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 0);

            assert!(guard.is_empty(range).await?);
        }

        let key = row2[..1].to_vec();
        let values = row2[1..].to_vec();

        // test inserting a second row
        {
            let mut guard = table.write().await;
            assert!(guard.upsert(key.clone(), values.clone()).await?);
            assert!(!guard.upsert(key, values).await?);
        }

        // test reading a range
        {
            let guard = table.read().await;

            assert_eq!(guard.count(Default::default()).await?, 2);

            let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range =
                Range::from_iter([("up_name".to_string(), Value::String("two".to_string()))]);

            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range = Range::from_iter([(
                "up".to_string(),
                (Bound::Included(1.into()), Bound::Excluded(5.into())),
            )]);

            assert_eq!(guard.count(range.clone()).await?, 2);
            assert!(!guard.is_empty(range).await?);
        }
    }

    // test reading a stream of all rows
    {
        let guard = table.read().await;
        let mut stream = guard.rows(Range::default(), &[], false, None)?;

        assert_eq!(
            stream.try_next().await?,
            Some(row1.iter().cloned().collect())
        );
        assert_eq!(
            stream.try_next().await?,
            Some(row2.iter().cloned().collect())
        );
        assert_eq!(stream.try_next().await?, None);

        let range = Range::from_iter([(
            "down".to_string(),
            (Bound::Unbounded, Bound::Excluded(10.into())),
        )]);

        let mut stream = guard.rows(range, &[], true, None)?;
        assert_eq!(
            stream.try_next().await?,
            Some(row1.iter().cloned().collect())
        );
        assert_eq!(
            stream.try_next().await?,
            Some(row2.iter().cloned().collect())
        );
        assert_eq!(stream.try_next().await?, None);
    }

    // test deleting a row
    {
        table.write().await.delete_row(smallvec![1.into()]).await?;

        let guard = table.read().await;
        assert_eq!(guard.count(Default::default()).await?, 1);

        let range = Range::from_iter([("up".to_string(), Value::Number(1.into()))]);
        assert_eq!(guard.count(range.clone()).await?, 0);
        assert!(guard.is_empty(range).await?);
    }

    // test deleting the last row
    {
        table.write().await.delete_row(smallvec![2.into()]).await?;

        let guard = table.read().await;
        assert_eq!(guard.count(Default::default()).await?, 0);
        assert!(guard.is_empty(Default::default()).await?);

        let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
        assert_eq!(guard.count(range.clone()).await?, 0);
        assert!(guard.is_empty(range).await?);
    }

    // clean up
    fs::remove_dir_all(path).await
}
