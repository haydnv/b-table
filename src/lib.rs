use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Bound, Deref};
use std::sync::Arc;
use std::{fmt, io};

use b_tree::{BTree, BTreeLock, Key};
use collate::{Collate, Overlap, OverlapsRange, OverlapsValue};
use freqfs::{Dir, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::stream::{self, Stream};
use safecast::AsType;

/// A node in a [`BTree`]
pub use b_tree::Node;

/// A read guard acquired on a [`TableLock`]
pub type TableReadGuard<S, IS, C, FE> = Table<S, IS, C, DirReadGuardOwned<FE>>;

/// A write guard acquired on a [`TableLock`]
pub type TableWriteGuard<S, IS, C, FE> = Table<S, IS, C, DirWriteGuardOwned<FE>>;

/// The schema of a [`Table`]
pub trait Schema {
    type Id: Hash + Eq;
    type Error: std::error::Error + From<io::Error>;
    type Value: Clone + Eq + fmt::Debug + 'static;
    type Index: b_tree::Schema<Error = Self::Error, Value = Self::Value>;

    /// List the columns of a [`Table`]
    fn columns(&self) -> &[Self::Id];

    /// Borrow the schema of the primary key
    fn primary(&self) -> &Self::Index;

    /// Borrow the schemata of the auxiliary keys
    fn auxiliary(&self) -> &[Self::Index];

    /// Check that the given `key` is a valid primary key
    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;

    /// Check that the given `values` are valid for a row in a table
    fn validate_values(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;
}

/// A range on a single column
pub enum ColumnRange<V> {
    Eq(V),
    In((Bound<V>, Bound<V>)),
}

impl<C> OverlapsRange<Self, C> for ColumnRange<C::Value>
where
    C: Collate,
    C::Value: fmt::Debug,
    std::ops::Range<C::Value>: OverlapsRange<std::ops::Range<C::Value>, C>,
{
    fn overlaps(&self, other: &Self, collator: &C) -> Overlap {
        match (self, other) {
            (Self::Eq(this), Self::Eq(that)) => match collator.cmp(this, that) {
                Ordering::Less => Overlap::Less,
                Ordering::Equal => Overlap::Equal,
                Ordering::Greater => Overlap::Greater,
            },
            (Self::In(this), Self::Eq(that)) => this.overlaps_value(that, collator),
            (Self::Eq(this), Self::In(that)) => match that.overlaps_value(this, collator) {
                Overlap::Less => Overlap::Greater,
                Overlap::WideLess | Overlap::Wide | Overlap::WideGreater => Overlap::Narrow,
                Overlap::Greater => Overlap::Less,
                Overlap::Equal => Overlap::Equal,
                Overlap::Narrow => unreachable!("{:?} is narrower than {:?}", that, this),
            },
            (Self::In(this), Self::In(that)) => this.overlaps(that, collator),
        }
    }
}

/// A range used in a where condition
pub struct Range<K, V> {
    columns: HashMap<K, ColumnRange<V>>,
}

impl<K, V> Default for Range<K, V> {
    fn default() -> Self {
        Self {
            columns: HashMap::with_capacity(0),
        }
    }
}

impl<K, V> Range<K, V> {
    /// Return `true` if this [`Range`] has no bounds.
    pub fn is_default(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get the number of columns specified by this range.
    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

impl<K: Eq + Hash, V> Range<K, V> {
    /// Get a [`ColumnRange`] in this range, if specified.
    pub fn get(&self, column: &K) -> Option<&ColumnRange<V>> {
        self.columns.get(column)
    }
}

impl<C, K> OverlapsRange<Self, C> for Range<K, C::Value>
where
    K: Eq + Hash,
    C: Collate,
    C::Value: fmt::Debug,
{
    fn overlaps(&self, other: &Self, collator: &C) -> Overlap {
        let mut overlap: Option<Overlap> = None;

        // handle the case that there is a column absent in this range but not the other
        for name in other.columns.keys() {
            if !self.columns.contains_key(name) {
                return Overlap::Wide;
            }
        }

        for (name, this) in &self.columns {
            let column_overlap = other
                .columns
                .get(name)
                .map(|that| this.overlaps(that, collator))
                // handle the case that there is a column present in this range but not the other
                .unwrap_or(Overlap::Narrow);

            if let Some(overlap) = overlap.as_mut() {
                *overlap = overlap.then(column_overlap);
            } else {
                overlap = Some(column_overlap);
            }
        }

        // handle the case that both ranges are empty
        overlap.unwrap_or(Overlap::Equal)
    }
}

/// A futures-aware read-write lock on a [`Table`]
pub struct TableLock<S, IS, C, FE> {
    schema: Arc<S>,
    dir: DirLock<FE>,
    primary: BTreeLock<IS, C, FE>,
    auxiliary: Vec<BTreeLock<IS, C, FE>>,
}

impl<S, IS, C, FE> Clone for TableLock<S, IS, C, FE> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            dir: self.dir.clone(),
            primary: self.primary.clone(),
            auxiliary: self.auxiliary.clone(),
        }
    }
}

impl<S, IS, C, FE> TableLock<S, IS, C, FE> {
    /// Borrow the [`Schema`] of this [`Table`]
    pub fn schema(&self) -> &S {
        &self.schema
    }
}

impl<S: Schema, C, FE> TableLock<S, S::Index, C, FE> {
    /// Create a new [`Table`]
    pub fn create(_schema: S, _collator: C, _dir: DirLock<FE>) -> Self {
        todo!()
    }

    /// Load an existing [`Table`] with the given `schema` from the given `dir`
    pub fn load(_schema: S, _collator: C, _dir: DirLock<FE>) -> Self {
        todo!()
    }
}

impl<S: Schema, C, FE> TableLock<S, S::Index, C, FE> {
    /// Lock this [`Table`] for reading
    pub async fn read(&self) -> TableReadGuard<S, S::Index, C, FE> {
        todo!()
    }

    /// Lock this [`Table`] for writing
    pub async fn write(&self) -> TableWriteGuard<S, S::Index, C, FE> {
        todo!()
    }
}

/// A database table with support for multiple indices
pub struct Table<S, IS, C, G> {
    schema: Arc<S>,
    primary: BTree<IS, C, G>,
    auxiliary: Vec<BTree<IS, C, G>>,
}

impl<S, C, FE, G> Table<S, S::Index, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    FE: FileLoad + AsType<Node<S::Value>>,
    G: Deref<Target = Dir<FE>> + 'static,
    Node<S::Value>: fmt::Debug,
{
    /// Count how many rows in this [`Table`] lie within the given `range`.
    pub async fn count(&self, _range: Range<S::Id, S::Value>) -> Result<u64, io::Error> {
        todo!()
    }

    /// Return `true` if the given `key` is present in this [`Table`].
    pub async fn contains(&self, key: Key<S::Value>) -> Result<bool, S::Error> {
        let _key = self.schema.validate_key(key)?;

        todo!()
    }

    /// Look up a row by its `key`.
    pub async fn get(&self, key: Key<S::Value>) -> Result<Option<Vec<S::Value>>, S::Error> {
        let _key = self.schema.validate_key(key)?;

        todo!()
    }

    /// Construct a [`Stream`] of the values of the `columns` of the rows within the given `range`.
    pub fn into_stream(
        self,
        _range: Range<S::Id, S::Value>,
        _columns: Vec<S::Id>,
        _reverse: bool,
    ) -> impl Stream<Item = Result<Vec<S::Value>, io::Error>> {
        // TODO
        stream::empty()
    }
}

impl<S, C, FE> Table<S, S::Index, C, DirWriteGuardOwned<FE>>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    FE: FileLoad + AsType<Node<S::Value>>,
    Node<S::Value>: fmt::Debug,
{
    /// Delete a row from this [`Table`] by its `key`.
    /// Returns `true` if the given `key` was present.
    pub async fn delete(&mut self, key: Vec<S::Value>) -> Result<bool, S::Error> {
        let _key = self.schema.validate_key(key)?;
        todo!()
    }

    /// Insert or update a row in this [`Table`].
    /// Returns `true` if a new row was inserted.
    pub async fn upsert(
        &mut self,
        key: Vec<S::Value>,
        values: Vec<S::Value>,
    ) -> Result<bool, S::Error> {
        let _key = self.schema.validate_key(key)?;
        let _values = self.schema.validate_values(values)?;

        todo!()
    }
}
