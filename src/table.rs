use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io};

use b_tree::collate::Collate;
use b_tree::{BTree, BTreeLock, Key};
use freqfs::{DirDeref, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::{Future, TryFutureExt};
use futures::stream::{Stream, TryStreamExt};
use futures::try_join;
use safecast::AsType;
use smallvec::SmallVec;

use super::plan::QueryPlan;
use super::schema::*;
use super::{IndexStack, Node};

const PRIMARY: &str = "primary";

/// The maximum number of values in a stack-allocated [`Row`]
pub const ROW_STACK_SIZE: usize = 32;

/// A read guard acquired on a [`TableLock`]
pub type TableReadGuard<S, IS, C, FE> = Table<S, IS, C, Arc<DirReadGuardOwned<FE>>>;

/// A write guard acquired on a [`TableLock`]
pub type TableWriteGuard<S, IS, C, FE> = Table<S, IS, C, DirWriteGuardOwned<FE>>;

/// The type of row returned in a [`Stream`] of [`Rows`]
pub type Row<V> = SmallVec<[V; ROW_STACK_SIZE]>;

/// A stream of table rows
pub type Rows<V> = Pin<Box<dyn Stream<Item = Result<Row<V>, io::Error>> + Send>>;

/// A futures-aware read-write lock on a [`Table`]
pub struct TableLock<S, IS, C, FE> {
    schema: Arc<TableSchema<S>>,
    dir: DirLock<FE>,
    primary: BTreeLock<IS, C, FE>,
    // use a BTreeMap to make sure index locks are always acquired in-order
    auxiliary: BTreeMap<String, BTreeLock<IS, C, FE>>,
}

impl<S, IS, C, FE> Clone for TableLock<S, IS, C, FE>
where
    C: Clone,
{
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
    /// Borrow the [`Schema`] of this [`Table`].
    pub fn schema(&self) -> &S {
        self.schema.inner()
    }

    /// Borrow the collator for this [`Table`].
    pub fn collator(&self) -> &b_tree::Collator<C> {
        self.primary.collator()
    }
}

impl<S, C, FE> TableLock<S, S::Index, C, FE>
where
    S: Schema,
    C: Clone,
    FE: AsType<Node<S::Value>> + Send + Sync,
    Node<S::Value>: FileLoad,
{
    /// Create a new [`Table`]
    pub fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        for (index_name, index) in schema.auxiliary() {
            for col_name in index.columns() {
                if !schema.primary().columns().contains(col_name) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("index {index_name} refers to unknown column {col_name}"),
                    ));
                }
            }

            for col_name in schema.key() {
                if !index.columns().contains(col_name) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("index {index_name} is missing primary key column {col_name}"),
                    ));
                }
            }
        }

        let mut dir_contents = dir.try_write()?;

        let primary = {
            let dir = dir_contents.create_dir(PRIMARY.to_string())?;
            BTreeLock::create(schema.primary().clone(), collator.clone(), dir)
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.create_dir(name.to_string())?;
                BTreeLock::create(schema.clone(), collator.clone(), dir)
            }?;

            auxiliary.insert(name.clone(), index);
        }

        std::mem::drop(dir_contents);

        Ok(Self {
            schema: Arc::new(schema.into()),
            primary,
            auxiliary,
            dir,
        })
    }

    /// Load an existing [`Table`] with the given `schema` from the given `dir`
    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        for (name, index) in schema.auxiliary() {
            for col_name in schema.key() {
                if !index.columns().contains(col_name) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("index {} is missing primary key column {}", name, col_name),
                    ));
                }
            }
        }

        let mut dir_contents = dir.try_write()?;

        let primary = {
            let dir = dir_contents.get_or_create_dir(PRIMARY.to_string())?;
            BTreeLock::load(schema.primary().clone(), collator.clone(), dir.clone())
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.get_or_create_dir(name.clone())?;
                BTreeLock::load(schema.clone(), collator.clone(), dir.clone())
            }?;

            auxiliary.insert(name.clone(), index);
        }

        std::mem::drop(dir_contents);

        Ok(Self {
            schema: Arc::new(schema.into()),
            primary,
            auxiliary,
            dir,
        })
    }

    pub async fn sync(&self) -> Result<(), io::Error>
    where
        FE: for<'a> freqfs::FileSave<'a>,
    {
        self.dir.sync().await
    }
}

impl<S, C, FE> TableLock<S, S::Index, C, FE>
where
    S: Schema,
    C: Clone,
    FE: Send + Sync,
    Node<S::Value>: FileLoad,
{
    /// Lock this [`Table`] for reading.
    pub async fn read(&self) -> TableReadGuard<S, S::Index, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.read().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for reading");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in &self.auxiliary {
            let index = index.read().await;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for reading");
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }

    /// Lock this [`Table`] for reading, without borrowing.
    pub async fn into_read(self) -> TableReadGuard<S, S::Index, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.into_read().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for reading");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary {
            let index = index.into_read().await;

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for reading");

            auxiliary.insert(name, index);
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }

    /// Lock this [`Table`] for reading synchronously, if possible.
    pub fn try_read(&self) -> Result<TableReadGuard<S, S::Index, C, FE>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.try_read()?;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for reading");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.iter() {
            let index = index.try_read()?;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for reading");
        }

        Ok(Table {
            schema,
            primary,
            auxiliary,
        })
    }

    /// Lock this [`Table`] for writing.
    pub async fn write(&self) -> TableWriteGuard<S, S::Index, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for writing...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.write().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for writing");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.iter() {
            let index = index.write().await;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for writing");
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }

    /// Lock this [`Table`] for writing, without borrowing.
    pub async fn into_write(self) -> TableWriteGuard<S, S::Index, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.into_write().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for writing");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.into_iter() {
            let index = index.into_write().await;

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for writing");

            auxiliary.insert(name, index);
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }

    /// Lock this [`Table`] for writing synchronously, if possible.
    pub fn try_write(&self) -> Result<TableWriteGuard<S, S::Index, C, FE>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for writing...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.try_write()?;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for writing");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.iter() {
            let index = index.try_write()?;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for writing");
        }

        Ok(Table {
            schema,
            primary,
            auxiliary,
        })
    }
}

/// A database table with support for multiple indices
pub struct Table<S, IS, C, G> {
    schema: Arc<TableSchema<S>>,
    primary: BTree<IS, C, G>,
    auxiliary: HashMap<String, BTree<IS, C, G>>,
}

impl<S, IS, C, G> Clone for Table<S, IS, C, G>
where
    C: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            primary: self.primary.clone(),
            auxiliary: self.auxiliary.clone(),
        }
    }
}

impl<S, C, G> Table<S, S::Index, C, G>
where
    S: Schema,
{
    #[inline]
    fn get_index<'a, Id>(&'a self, index_id: Id) -> Option<&'a BTree<S::Index, C, G>>
    where
        IndexId<'a>: From<Id>,
    {
        match index_id.into() {
            IndexId::Primary => Some(&self.primary),
            IndexId::Auxiliary(index_id) => self.auxiliary.get(index_id),
        }
    }
}

impl<S, C, FE, G> Table<S, S::Index, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + 'static,
    Node<S::Value>: FileLoad,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Return `true` if the given `key` is present in this [`Table`].
    pub async fn contains(&self, key: &[S::Value]) -> Result<bool, io::Error> {
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.primary.contains(key).await
        } else {
            Err(bad_key(key, key_len))
        }
    }

    /// Return the first row in the given `range`, if any.
    pub async fn first(
        &self,
        range: Range<S::Id, S::Value>,
    ) -> Result<Option<Row<S::Value>>, io::Error> {
        todo!()
    }

    /// Look up a row by its `key`.
    pub async fn get_row<K: Into<b_tree::Key<S::Value>>>(
        &self,
        key: K,
    ) -> Result<Option<Row<S::Value>>, io::Error> {
        let key = key.into();
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.primary.first(&b_tree::Range::from_prefix(key)).await
        } else {
            Err(bad_key(&key, key_len))
        }
    }

    /// Look up a value by its `key`.
    pub async fn get_value<K: Into<b_tree::Key<S::Value>>>(
        &self,
        key: K,
    ) -> Result<Option<Row<S::Value>>, io::Error> {
        let key = key.into();
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.primary
                .first(&b_tree::Range::from_prefix(key))
                .map_ok(|maybe_row| maybe_row.map(|mut row| row.drain(key_len..).collect()))
                .await
        } else {
            Err(bad_key(&key, key_len))
        }
    }
}

impl<S, C, FE, G> Table<S, S::Index, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<S::Value>: FileLoad,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Count how many rows in this [`Table`] lie within the given `range`.
    pub async fn count(&self, range: Range<S::Id, S::Value>) -> Result<u64, io::Error> {
        if range.is_default() {
            self.primary.count(&b_tree::Range::default()).await
        } else {
            // TODO: optimize
            let mut rows = self.rows(range, &[], false, None).await?;

            let mut count = 0;
            while let Some(_row) = rows.try_next().await? {
                count += 1;
            }

            Ok(count)
        }
    }

    /// Return `true` if the given [`Range`] of this [`Table`] does not contain any rows.
    pub async fn is_empty(&self, range: Range<S::Id, S::Value>) -> Result<bool, io::Error> {
        if range.is_default() {
            self.primary.is_empty(&b_tree::Range::default()).await
        } else {
            let mut rows = self.rows(range, &[], false, None).await?;

            rows.try_next()
                .map_ok(|maybe_row| maybe_row.is_none())
                .await
        }
    }

    /// Construct a [`Stream`] of the `select`ed columns of the [`Rows`] within the given `range`.
    pub async fn rows(
        &self,
        range: Range<S::Id, S::Value>,
        order: &[S::Id],
        reverse: bool,
        select: Option<&[S::Id]>,
    ) -> Result<Rows<S::Value>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::rows with order {order:?}");

        let mut range = range.into_inner();

        let mut plan = QueryPlan::new(&self.schema, &range, order).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{self:?} has no index to query {range:?} with order {order:?}"),
            )
        })?;

        let keys: b_tree::Keys<S::Value> = Box::pin(futures::stream::empty());
        let index_columns = Option::<&[S::Id]>::None;

        // if there is only one index, construct a stream over its keys with the given range
        // otherwise:
        //  construct a stream over the unique prefixes in the first index
        //  for each index before the last, merge all unique prefixes beginning with each prefix
        //  merge streams of all keys in the last index beginning with each prefix

        // if all columns to select are already present, return the stream
        // otherwise, construct a stream of rows by extracting & selecting each primary key

        todo!()
    }

    /// Consume this [`TableReadGuard`] to construct a [`Stream`] of all the rows in the [`Table`].
    pub async fn into_rows(self) -> Result<Rows<S::Value>, io::Error> {
        let rows = self.primary.keys(b_tree::Range::default(), false).await?;
        Ok(Box::pin(rows))
    }
}

impl<S: fmt::Debug, IS, C, G> fmt::Debug for Table<S, IS, C, G> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "table with schema {:?}", self.schema.inner())
    }
}

#[inline]
fn index_range_for<K: Eq + Hash, V>(
    columns: &[&K],
    range: &mut HashMap<K, ColumnRange<V>>,
) -> b_tree::Range<V> {
    let mut prefix = b_tree::Key::with_capacity(range.len());

    for col_name in columns {
        if let Some(col_range) = range.remove(col_name) {
            match col_range {
                ColumnRange::Eq(value) => {
                    prefix.push(value);
                }
                ColumnRange::In(bounds) => {
                    return b_tree::Range::with_bounds(prefix, bounds);
                }
            }
        }
    }

    b_tree::Range::from_prefix(prefix)
}

#[inline]
fn inner_range<V>(
    mut prefix: b_tree::Key<V>,
    column_range: Option<ColumnRange<V>>,
) -> b_tree::Range<V> {
    if let Some(column_range) = column_range {
        match column_range {
            ColumnRange::Eq(value) => {
                prefix.push(value);
                b_tree::Range::from_prefix(prefix)
            }
            ColumnRange::In(bounds) => b_tree::Range::with_bounds(prefix, bounds),
        }
    } else {
        b_tree::Range::from_prefix(prefix)
    }
}

fn prefix_extractor<K, V>(
    columns_in: &[&K],
    columns_out: &[K],
) -> Box<dyn Fn(Key<V>) -> Key<V> + Send>
where
    K: PartialEq + fmt::Debug,
{
    debug_assert!(columns_out.len() <= columns_in.len());
    debug_assert!(!columns_out.is_empty());
    debug_assert!(
        columns_out.iter().all(|id| columns_in.contains(&id)),
        "{columns_out:?} is not a subset of {columns_in:?}"
    );

    #[cfg(feature = "logging")]
    log::trace!("extract columns {columns_out:?} from {columns_in:?}");

    if columns_in.len()
        == columns_in
            .iter()
            .zip(columns_out)
            .filter(|(i, o)| *i == o)
            .count()
    {
        return Box::new(|key| key);
    }

    let mut indices = IndexStack::with_capacity(columns_out.len());

    for name_out in columns_out {
        let mut index = columns_in
            .iter()
            .position(|name_in| *name_in == name_out)
            .expect("index");

        index -= indices.iter().copied().filter(|i| *i < index).count();

        indices.push(index);
    }

    Box::new(move |mut key| {
        let mut prefix = b_tree::Key::with_capacity(indices.len() + 1);

        for i in indices.iter().copied() {
            prefix.push(key.remove(i));
        }

        prefix
    })
}

impl<S, IS, C, FE> Table<S, IS, C, DirWriteGuardOwned<FE>> {
    /// Downgrade this write lock to a read lock.
    pub fn downgrade(self) -> Table<S, IS, C, Arc<DirReadGuardOwned<FE>>> {
        Table {
            schema: self.schema,
            primary: self.primary.downgrade(),
            auxiliary: self
                .auxiliary
                .into_iter()
                .map(|(name, index)| (name, index.downgrade()))
                .collect(),
        }
    }
}

impl<S, C, FE> Table<S, S::Index, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    <S as Schema>::Index: Send + Sync,
    Node<S::Value>: FileLoad,
{
    /// Delete a row from this [`Table`] by its `key`.
    /// Returns `true` if the given `key` was present.
    pub async fn delete_row(&mut self, key: b_tree::Key<S::Value>) -> Result<bool, S::Error> {
        let row = if let Some(row) = self.get_row(key).await? {
            row
        } else {
            return Ok(false);
        };

        let mut deletes = IndexStack::with_capacity(self.auxiliary.len() + 1);

        for (name, index) in self.auxiliary.iter_mut() {
            deletes.push(async {
                let index_key = self.schema.extract_key(name.into(), &row);
                index.delete(&index_key).await
            })
        }

        self.primary.delete(&row).await?;

        for present in try_join_all(deletes).await? {
            assert!(present, "table index is out of sync");
        }

        Ok(true)
    }

    /// Delete all rows in the given `range` from this [`Table`].
    pub async fn delete_range(&mut self, range: Range<S::Id, S::Value>) -> Result<usize, S::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::delete_range");

        todo!()
    }

    /// Delete all rows from the `other` table from this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn delete_all(
        &mut self,
        mut other: TableReadGuard<S, S::Index, C, FE>,
    ) -> Result<(), S::Error> {
        // no need to check the collator for equality, that will be done in the index operations

        // but do check that the indices to merge are the same
        if self.schema != other.schema {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot delete the contents of a table with schema {:?} from one with schema {:?}",
                    other.schema.inner(), self.schema.inner()
                ),
            )
            .into());
        }

        let mut deletes = IndexStack::with_capacity(self.auxiliary.len() + 1);

        deletes.push(self.primary.delete_all(other.primary));

        for (name, this) in self.auxiliary.iter_mut() {
            let that = other.auxiliary.remove(name).expect("other index");
            deletes.push(this.delete_all(that));
        }

        try_join_all(deletes).await?;

        Ok(())
    }

    /// Insert all rows from the `other` table into this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn merge(
        &mut self,
        mut other: TableReadGuard<S, S::Index, C, FE>,
    ) -> Result<(), S::Error> {
        // no need to check the collator for equality, that will be done in the merge operations

        // but do check that the indices to merge are the same
        if self.schema != other.schema {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot merge a table with schema {:?} into one with schema {:?}",
                    other.schema.inner(),
                    self.schema.inner()
                ),
            )
            .into());
        }

        let mut merges = IndexStack::with_capacity(self.auxiliary.len() + 1);

        merges.push(self.primary.merge(other.primary));

        for (name, this) in self.auxiliary.iter_mut() {
            let that = other.auxiliary.remove(name).expect("other index");
            merges.push(this.merge(that));
        }

        try_join_all(merges).await?;

        Ok(())
    }

    /// Insert or update a row in this [`Table`].
    /// Returns `true` if a new row was inserted.
    pub async fn upsert(
        &mut self,
        key: Vec<S::Value>,
        values: Vec<S::Value>,
    ) -> Result<bool, S::Error> {
        let key = self.schema.inner().validate_key(key)?;
        let values = self.schema.inner().validate_values(values)?;

        let mut row = key;
        row.extend(values);

        let mut inserts = IndexStack::with_capacity(self.auxiliary.len() + 1);

        for (name, index) in self.auxiliary.iter_mut() {
            let index_key = self.schema.extract_key(name.into(), &row);
            inserts.push(index.insert(index_key.into_vec()));
        }

        inserts.push(self.primary.insert(row));

        let mut inserts = try_join_all(inserts).await?;
        let new = inserts.pop().expect("insert");
        while let Some(index_new) = inserts.pop() {
            assert_eq!(new, index_new, "index out of sync");
        }

        Ok(new)
    }

    /// Delete all rows from this [`Table`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::truncate");

        let mut truncates = IndexStack::with_capacity(self.auxiliary.len() + 1);
        truncates.push(self.primary.truncate());

        for index in self.auxiliary.values_mut() {
            truncates.push(index.truncate());
        }

        try_join_all(truncates).await?;

        Ok(())
    }
}

#[inline]
fn bad_key<V: fmt::Debug>(key: &[V], key_len: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid key: {key:?}, expected exactly {key_len} column(s)",),
    )
}

#[inline]
fn try_join_all<'a, O, F>(
    mut futures: IndexStack<F>,
) -> Pin<Box<dyn Future<Output = Result<IndexStack<O>, io::Error>> + Send + 'a>>
where
    O: Send + 'a,
    F: Future<Output = Result<O, io::Error>> + Send + 'a,
{
    Box::pin(async move {
        if let Some(fut) = futures.pop() {
            let (out, mut others) = try_join!(fut, try_join_all(futures))?;
            others.push(out);
            Ok(others)
        } else {
            Ok(IndexStack::new())
        }
    })
}
