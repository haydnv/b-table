use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Bound, Deref};
use std::sync::Arc;
use std::{fmt, io, iter};

use b_tree::{BTree, BTreeLock, Key};
use collate::{Collate, Overlap, OverlapsRange, OverlapsValue};
use freqfs::{Dir, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::{join_all, try_join_all, TryFutureExt};
use futures::stream::{self, Stream, TryStreamExt};
use safecast::AsType;

const PRIMARY: &str = "primary";

/// A node in a [`BTree`]
pub use b_tree::Node;

/// A read guard acquired on a [`TableLock`]
pub type TableReadGuard<S, IS, C, FE> = Table<S, IS, C, DirReadGuardOwned<FE>>;

/// A write guard acquired on a [`TableLock`]
pub type TableWriteGuard<S, IS, C, FE> = Table<S, IS, C, DirWriteGuardOwned<FE>>;

/// The schema of a [`Table`] index
pub trait IndexSchema: b_tree::Schema + Clone {
    type Id: Hash + Eq;

    /// Borrow the list of columns specified by this schema.
    fn columns(&self) -> &[Self::Id];

    /// Given a key matching this [`Schema`], extract a key matching the `other` [`Schema`].
    /// This values in `key` must be in order, but the values in `other` may be in any order.
    /// Panics: if `other` is not a subset of `self`.
    fn extract_key(&self, key: &[Self::Value], other: &Self) -> Key<Self::Value>;

    /// Extract a [`b_tree::Range`] from the given [`Range`] if it matches this [`Schema`].
    fn extract_range(
        &self,
        range: Range<Self::Id, Self::Value>,
    ) -> Option<b_tree::Range<Self::Value>> {
        if range.is_default() {
            return Some(b_tree::Range::default());
        }

        let columns = self.columns();
        let mut range = range.into_inner();

        let mut prefix = Vec::with_capacity(columns.len());
        let mut i = 0;

        let index_range = loop {
            if let Some(column) = columns.get(i) {
                match range.remove(&column) {
                    None => break b_tree::Range::from_prefix(prefix),
                    Some(ColumnRange::Eq(value)) => {
                        prefix.push(value);
                        i += 1;
                    }
                    Some(ColumnRange::In((start, end))) => {
                        break b_tree::Range::with_bounds(prefix, (start, end));
                    }
                }
            } else {
                break b_tree::Range::from_prefix(prefix);
            }
        };

        if range.is_empty() {
            Some(index_range)
        } else {
            None
        }
    }

    /// Return `true` if a [`BTree`] with this [`Schema`] supports the given [`Range`].
    fn supports(&self, range: &Range<Self::Id, Self::Value>) -> bool {
        let columns = self.columns();
        let mut i = 0;

        while i < columns.len() {
            match range.get(&columns[i]) {
                None => break,
                Some(ColumnRange::Eq(_)) => i += 1,
                Some(ColumnRange::In(_)) => {
                    i += 1;
                    break;
                }
            }
        }

        i == range.len()
    }
}

/// The schema of a [`Table`]
pub trait Schema {
    type Id: Hash + Eq;
    type Error: std::error::Error + From<io::Error>;
    type Value: Clone + Eq + fmt::Debug + 'static;
    type Index: IndexSchema<Error = Self::Error, Id = Self::Id, Value = Self::Value>;

    /// Borrow the schema of the primary index.
    fn primary(&self) -> &Self::Index;

    /// Borrow the schemata of the auxiliary indices.
    /// This is ordered so that the first index which matches a given [`Range`] will be used.
    fn auxiliary(&self) -> &[(String, Self::Index)];

    /// Check that the given `key` is a valid primary key for a [`Table`] with this [`Schema`].
    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;

    /// Check that the given `values` are valid for a row in a [`Table`] with this [`Schema`].
    fn validate_values(&self, values: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;
}

/// A range on a single column
#[derive(Copy, Clone, Eq, PartialEq)]
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
                Overlap::Equal => Overlap::Equal,

                Overlap::Less => Overlap::Greater,
                Overlap::WideLess => Overlap::WideGreater,
                Overlap::Wide => Overlap::Narrow,
                Overlap::WideGreater => Overlap::WideLess,
                Overlap::Greater => Overlap::Less,

                Overlap::Narrow => unreachable!("{:?} is narrower than {:?}", that, this),
            },
            (Self::In(this), Self::In(that)) => this.overlaps(that, collator),
        }
    }
}

impl<V> From<V> for ColumnRange<V> {
    fn from(value: V) -> Self {
        Self::Eq(value)
    }
}

impl<V> From<(Bound<V>, Bound<V>)> for ColumnRange<V> {
    fn from(bounds: (Bound<V>, Bound<V>)) -> Self {
        Self::In(bounds)
    }
}

impl<V: fmt::Debug> fmt::Debug for ColumnRange<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Eq(value) => write!(f, "{:?}", value),
            Self::In((start, end)) => {
                match start {
                    Bound::Unbounded => f.write_str("[."),
                    Bound::Included(start) => write!(f, "[{start:?}."),
                    Bound::Excluded(start) => write!(f, "({start:?}."),
                }?;

                match end {
                    Bound::Unbounded => f.write_str(".]"),
                    Bound::Included(end) => write!(f, ".{end:?}]"),
                    Bound::Excluded(end) => write!(f, ".{end:?})"),
                }
            }
        }
    }
}

/// A range used in a where condition
#[derive(Clone)]
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
    /// Destructure this [`Range`] into a [`HashMap`] of [`ColumnRanges`].
    pub fn into_inner(self) -> HashMap<K, ColumnRange<V>> {
        self.columns
    }

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

impl<K: Hash + Eq, V> FromIterator<(K, V)> for Range<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self {
            columns: iter
                .into_iter()
                .map(|(name, bound)| (name, ColumnRange::Eq(bound)))
                .collect(),
        }
    }
}

impl<K: Hash + Eq, V> FromIterator<(K, (Bound<V>, Bound<V>))> for Range<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, (Bound<V>, Bound<V>))>>(iter: I) -> Self {
        Self {
            columns: iter
                .into_iter()
                .map(|(name, bounds)| (name, ColumnRange::In(bounds)))
                .collect(),
        }
    }
}

impl<K, V> fmt::Debug for Range<K, V>
where
    K: fmt::Display,
    ColumnRange<V>: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;

        write!(
            f,
            "{}",
            self.columns
                .iter()
                .map(|(column, bound)| format!("{column}: {bound:?}"))
                .collect::<Vec<String>>()
                .join(", ")
        )?;

        f.write_str("}")
    }
}

/// A futures-aware read-write lock on a [`Table`]
pub struct TableLock<S, IS, C, FE> {
    schema: Arc<S>,
    dir: DirLock<FE>,
    primary: BTreeLock<IS, C, FE>,
    auxiliary: HashMap<String, BTreeLock<IS, C, FE>>,
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

impl<S, C, FE> TableLock<S, S::Index, C, FE>
where
    S: Schema,
    C: Clone,
    FE: FileLoad + AsType<Node<Vec<Key<S::Value>>>>,
{
    /// Create a new [`Table`]
    pub fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        let mut dir_contents = dir.try_write()?;

        let primary = {
            let dir = dir_contents.create_dir(PRIMARY.to_string())?;
            BTreeLock::create(schema.primary().clone(), collator.clone(), dir)
        }?;

        let mut auxiliary = HashMap::with_capacity(schema.auxiliary().len());
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.create_dir(name.to_string())?;
                BTreeLock::create(schema.clone(), collator.clone(), dir)
            }?;

            auxiliary.insert(name.clone(), index);
        }

        std::mem::drop(dir_contents);

        Ok(Self {
            schema: Arc::new(schema),
            primary,
            auxiliary,
            dir,
        })
    }

    /// Load an existing [`Table`] with the given `schema` from the given `dir`
    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        let dir_contents = dir.try_read()?;

        let primary = {
            let dir = dir_contents
                .get_dir(PRIMARY)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "primary table index"))?;

            BTreeLock::load(schema.primary().clone(), collator.clone(), dir.clone())
        }?;

        let mut auxiliary = HashMap::with_capacity(schema.auxiliary().len());
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.get_dir(name).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, format!("table index {}", name))
                })?;

                BTreeLock::load(schema.clone(), collator.clone(), dir.clone())
            }?;

            auxiliary.insert(name.clone(), index);
        }

        std::mem::drop(dir_contents);

        Ok(Self {
            schema: Arc::new(schema),
            primary,
            auxiliary,
            dir,
        })
    }
}

impl<S: Schema, C, FE: FileLoad> TableLock<S, S::Index, C, FE> {
    /// Lock this [`Table`] for reading
    pub async fn read(&self) -> TableReadGuard<S, S::Index, C, FE> {
        // lock the primary key first, separately from the indices, to avoid a deadlock
        Table {
            schema: self.schema.clone(),
            primary: self.primary.read().await,
            auxiliary: join_all(self.auxiliary.values().map(|index| index.read())).await,
        }
    }

    /// Lock this [`Table`] for writing
    pub async fn write(&self) -> TableWriteGuard<S, S::Index, C, FE> {
        // lock the primary key first, separately from the indices, to avoid a deadlock
        Table {
            schema: self.schema.clone(),
            primary: self.primary.write().await,
            auxiliary: join_all(self.auxiliary.values().map(|index| index.write())).await,
        }
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
    FE: FileLoad + AsType<Node<Vec<Vec<S::Value>>>>,
    G: Deref<Target = Dir<FE>> + 'static,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Count how many rows in this [`Table`] lie within the given `range`.
    pub async fn count(&self, range: Range<S::Id, S::Value>) -> Result<u64, io::Error> {
        for index in iter::once(&self.primary).chain(self.auxiliary.iter()) {
            if index.schema().supports(&range) {
                let range = index.schema().extract_range(range).expect("range");
                return index.count(&range).await;
            }
        }

        // todo: count rows returned by Table::to_stream if possible
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "this table has no index to support the given range: {:?}",
                range
            ),
        ))
    }

    /// Return `true` if the given `key` is present in this [`Table`].
    pub async fn contains(&self, key: Key<S::Value>) -> Result<bool, S::Error> {
        let key = self.schema.validate_key(key)?;
        let range = b_tree::Range::from_prefix(key);

        self.primary
            .is_empty(&range)
            .map_ok(|empty| !empty)
            .map_err(S::Error::from)
            .await
    }

    /// Look up a row by its `key`.
    pub async fn get(&self, key: Key<S::Value>) -> Result<Option<Vec<S::Value>>, S::Error> {
        let key = self.schema.validate_key(key)?;
        let range = b_tree::Range::from_prefix(key);
        let mut keys = self.primary.to_stream(&range);
        if let Some(block) = keys.try_next().await? {
            Ok(block.first().cloned())
        } else {
            Ok(None)
        }
    }

    /// Return `true` if the given [`Range`] of this [`Table`] does not contain any rows.
    pub async fn is_empty(&self, range: Range<S::Id, S::Value>) -> Result<bool, io::Error> {
        for index in iter::once(&self.primary).chain(self.auxiliary.iter()) {
            if index.schema().supports(&range) {
                let range = index.schema().extract_range(range).expect("range");
                return index.is_empty(&range).await;
            }
        }

        // todo: count rows returned by Table::to_stream if possible
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "this table has no index to support the given range: {:?}",
                range
            ),
        ))
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
    FE: FileLoad + AsType<Node<Vec<Vec<S::Value>>>>,
    Node<S::Value>: fmt::Debug,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Delete a row from this [`Table`] by its `key`.
    /// Returns `true` if the given `key` was present.
    pub async fn delete(&mut self, key: Vec<S::Value>) -> Result<bool, S::Error> {
        let key = self.schema.validate_key(key)?;
        let row = if let Some(row) = self.get(key).await? {
            row
        } else {
            return Ok(false);
        };

        let mut deletes = Vec::with_capacity(self.auxiliary.len() + 1);

        for index in &mut self.auxiliary {
            let row = IndexSchema::extract_key(self.schema.primary(), &row, index.schema());
            deletes.push(index.delete(row));
        }

        deletes.push(self.primary.delete(row));

        for present in try_join_all(deletes).await? {
            assert!(present, "table index is out of sync");
        }

        Ok(true)
    }

    /// Insert or update a row in this [`Table`].
    /// Returns `true` if a new row was inserted.
    pub async fn upsert(
        &mut self,
        key: Vec<S::Value>,
        values: Vec<S::Value>,
    ) -> Result<bool, S::Error> {
        let key = self.schema.validate_key(key)?;
        let values = self.schema.validate_values(values)?;

        let mut row = key;
        row.extend(values);

        let mut inserts = Vec::with_capacity(self.auxiliary.len() + 1);

        for index in &mut self.auxiliary {
            let row = IndexSchema::extract_key(self.schema.primary(), &row, index.schema());
            inserts.push(index.insert(row));
        }

        inserts.push(self.primary.insert(row));

        let mut inserts = try_join_all(inserts).await?;
        let new = inserts.pop().expect("insert");
        while let Some(index_new) = inserts.pop() {
            assert_eq!(new, index_new, "index out of sync");
        }

        Ok(new)
    }
}
