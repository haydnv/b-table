use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io, mem};

use b_tree::collate::Collate;
use b_tree::{BTree, BTreeLock, Key};
use freqfs::{DirDeref, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::{try_join_all, TryFutureExt};
use futures::stream::{Stream, TryStreamExt};
use safecast::AsType;
use smallvec::{smallvec, SmallVec};

use super::plan::{IndexQuery, QueryPlan};
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
    auxiliary: BTreeMap<Arc<str>, BTreeLock<IS, C, FE>>,
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
        valid_schema(&schema)?;

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

            auxiliary.insert(name.clone().into(), index);
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
        valid_schema(&schema)?;

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

            auxiliary.insert(name.clone().into(), index);
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
            state: TableState { auxiliary, primary },
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
            state: TableState { auxiliary, primary },
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
            state: TableState { auxiliary, primary },
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
            state: TableState { auxiliary, primary },
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
            state: TableState { auxiliary, primary },
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
            state: TableState { auxiliary, primary },
        })
    }
}

struct TableState<IS, C, G> {
    // IMPORTANT! the auxiliary field must go before primary so that it will be dropped first
    auxiliary: HashMap<Arc<str>, BTree<IS, C, G>>,
    primary: BTree<IS, C, G>,
}

impl<IS, C: Clone, G: Clone> Clone for TableState<IS, C, G> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary.clone(),
            auxiliary: self.auxiliary.clone(),
        }
    }
}

impl<IS, C, G> TableState<IS, C, G> {
    #[inline]
    fn get_index<'a, Id>(&'a self, index_id: Id) -> Option<&'a BTree<IS, C, G>>
    where
        IndexId<'a>: From<Id>,
    {
        match index_id.into() {
            IndexId::Primary => Some(&self.primary),
            IndexId::Auxiliary(index_id) => self.auxiliary.get(index_id),
        }
    }
}
impl<IS, C, FE, G> TableState<IS, C, G>
where
    IS: IndexSchema,
    C: Collate<Value = IS::Value> + 'static,
    FE: AsType<Node<IS::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + 'static,
    Node<IS::Value>: FileLoad,
    Range<IS::Id, IS::Value>: fmt::Debug,
{
    async fn contains(&self, prefix: &[IS::Value]) -> Result<bool, io::Error> {
        self.primary.contains(prefix).await
    }

    async fn get_row(&self, key: &[IS::Value]) -> Result<Option<Row<IS::Value>>, io::Error> {
        self.primary.first(b_tree::Range::from_prefix(key)).await
    }

    async fn first<'a>(
        &self,
        plan: &QueryPlan<'a, IS::Id>,
        range: &HashMap<IS::Id, ColumnRange<IS::Value>>,
        select: &[IS::Id],
        key_columns: &[IS::Id],
    ) -> Result<Option<Row<IS::Value>>, io::Error> {
        let mut plan = plan.indices.iter();

        let (mut first, mut columns) = if let Some((index_id, _query)) = plan.next() {
            let index = self.get_index(*index_id).expect("index");
            let columns = index.schema().columns();
            let index_range = index_range_borrow(columns, range);

            if let Some(first) = index.first(index_range).await? {
                (first, columns)
            } else {
                return Ok(None);
            }
        } else {
            let index_range = index_range_borrow(self.primary.schema().columns(), range);

            return self
                .primary
                .first(index_range)
                .map_ok(|first| {
                    first.map(|first| {
                        extract_columns(first, self.primary.schema().columns(), select)
                    })
                })
                .await;
        };

        for (index_id, _query) in plan {
            let index = self.get_index(*index_id).expect("index");

            columns = index.schema().columns();

            let index_range = index_range_borrow(&columns, range);

            first = if let Some(key) = index.first(index_range).await? {
                key
            } else {
                return Ok(None);
            }
        }

        if !select.iter().all(|col_name| columns.contains(col_name)) {
            let pk = extract_columns(first, columns, key_columns);

            first = self
                .get_row(&pk)
                .map_ok(|maybe_row| maybe_row.expect("row"))
                .await?;

            columns = self.primary.schema().columns();
        }

        Ok(Some(extract_columns(first, columns, select)))
    }
}

impl<IS, C, FE, G> TableState<IS, C, G>
where
    IS: IndexSchema,
    C: Collate<Value = IS::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<IS::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<IS::Value>: FileLoad,
    Range<IS::Id, IS::Value>: fmt::Debug,
{
    async fn count<'a>(
        &'a self,
        plan: QueryPlan<'a, IS::Id>,
        range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        key_columns: &'a [IS::Id],
    ) -> Result<u64, io::Error> {
        // TODO: optimize
        let mut rows = self
            .rows(plan, range, false, key_columns, key_columns)
            .await?;

        let mut count = 0;
        while let Some(_row) = rows.try_next().await? {
            count += 1;
        }

        Ok(count)
    }

    async fn is_empty<'a>(
        &'a self,
        plan: QueryPlan<'a, IS::Id>,
        range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        key_columns: &'a [IS::Id],
    ) -> Result<bool, io::Error> {
        self.first(&plan, &range, key_columns, key_columns)
            .map_ok(|maybe_row| maybe_row.is_none())
            .await
    }

    // note: it would be clearer to implement this recursively but it would require removing the lifetime parameter
    async fn rows<'a>(
        &'a self,
        mut plan: QueryPlan<'a, IS::Id>,
        mut range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        reverse: bool,
        select: &'a [IS::Id],
        key_columns: &'a [IS::Id],
    ) -> Result<Rows<IS::Value>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("construct row stream with plan {plan:?}");

        let mut keys: Option<(b_tree::Keys<IS::Value>, &'a [IS::Id])> = None;

        let last_query = plan.indices.pop();

        if let Some((index_id, query)) = plan.indices.first() {
            assert_eq!(query.prefix_len(), 0);
            let index = self.get_index(*index_id).expect("index");

            let columns = &index.schema().columns()[..query.selected()];
            assert!(query.range().iter().zip(columns).all(|(r, c)| *r == c));

            let index_range = index_range_for(&columns[..query.range().len()], &mut range);
            let index_prefixes = index
                .clone()
                .groups(index_range, columns.len(), reverse)
                .await?;

            keys = Some((index_prefixes, columns));
        }

        // for each index before the last
        for (index_id, query) in plan.indices.into_iter().skip(1) {
            // merge all unique prefixes beginning with each prefix

            let index = self.get_index(index_id).expect("index");

            let (prefixes, columns_in) = keys.take().expect("prefixes");

            let columns_out = &index.schema().columns()[..query.selected()];

            assert_eq!(query.prefix_len(), columns_in.len());
            assert!(columns_out.len() > columns_in.len());
            assert!(query.range().iter().zip(columns_out).all(|(r, c)| *r == c));

            debug_assert!(columns_out
                .iter()
                .take(columns_in.len())
                .all(|c| columns_in.contains(c)));

            let extract_prefix = prefix_extractor(columns_in, &columns_out[..columns_in.len()]);

            let inner_range = inner_range_for(&query, &mut range);

            let n = columns_out.len();
            let index = index.clone();

            let index_prefixes = prefixes
                .map_ok(extract_prefix)
                .map_ok(move |prefix| inner_range.clone().prepend(prefix))
                .map_ok(move |index_range| {
                    let index = index.clone();
                    async move { index.groups(index_range, n, reverse).await }
                })
                .try_buffered(num_cpus::get())
                .try_flatten();

            keys = Some((Box::pin(index_prefixes), columns_out))
        }

        if let Some((index_id, query)) = last_query {
            if let Some((prefixes, columns_in)) = keys.take() {
                // merge streams of all keys in the last index beginning with each prefix

                assert_eq!(query.prefix_len(), columns_in.len());

                let index = self.get_index(index_id).expect("index");

                let columns_out = &index.schema().columns();

                debug_assert!(
                    columns_out.len() > columns_in.len(),
                    "cannot select {columns_out:?} with prefix {columns_in:?}"
                );

                debug_assert!(columns_out
                    .iter()
                    .take(columns_in.len())
                    .all(|c| columns_in.contains(c)));

                let extract_prefix = prefix_extractor(columns_in, &columns_out[..columns_in.len()]);

                let inner_range = inner_range_for(&query, &mut range);

                let index = index.clone();

                let index_keys = prefixes
                    .map_ok(extract_prefix)
                    .map_ok(move |prefix| inner_range.clone().prepend(prefix))
                    .map_ok(move |index_range| {
                        let index = index.clone();
                        async move { index.keys(index_range, reverse).await }
                    })
                    .try_buffered(num_cpus::get())
                    .try_flatten();

                keys = Some((Box::pin(index_keys), columns_out))
            } else {
                let index = self.get_index(index_id).expect("index");
                let columns = index.schema().columns();

                let index_range = index_range_for(columns, &mut range);
                assert!(range.is_empty());

                let index_keys = index.clone().keys(index_range, reverse).await?;
                keys = Some((Box::pin(index_keys), columns));
            }
        }

        let (keys, columns) = if let Some((keys, columns)) = keys {
            if select.iter().all(|c| columns.contains(c)) {
                // if all columns to select are already present, return the stream
                (keys, columns)
            } else {
                // otherwise, construct a stream of rows by extracting & selecting each primary key

                let index = self.primary.clone();
                let extract_prefix = prefix_extractor(columns, key_columns);

                let rows = keys
                    .map_ok(extract_prefix)
                    .map_ok(move |primary_key| {
                        let index = index.clone();
                        async move { index.first(b_tree::Range::from(primary_key)).await }
                    })
                    .try_buffered(num_cpus::get())
                    .map_ok(|maybe_row| maybe_row.expect("row"));

                let rows: Rows<IS::Value> = Box::pin(rows);
                (rows, self.primary.schema().columns())
            }
        } else {
            let columns = self.primary.schema().columns();
            let index_range = index_range_for(columns, &mut range);
            assert!(range.is_empty());
            let keys = self.primary.clone().keys(index_range, reverse).await?;
            (keys, columns)
        };

        if columns == select {
            Ok(keys)
        } else {
            let extract_prefix = prefix_extractor(columns, select);
            let rows = keys.map_ok(extract_prefix);
            Ok(Box::pin(rows))
        }
    }
}

#[inline]
fn index_range_borrow<'a, K: Eq + Hash, V>(
    columns: &[K],
    range: &'a HashMap<K, ColumnRange<V>>,
) -> b_tree::Range<&'a V> {
    let mut prefix = Key::with_capacity(range.len());

    for col_name in columns {
        if let Some(col_range) = range.get(col_name) {
            match col_range {
                ColumnRange::Eq(value) => {
                    prefix.push(value);
                }
                ColumnRange::In((start, end)) => {
                    return b_tree::Range::with_bounds(prefix, (start.as_ref(), end.as_ref()));
                }
            }
        } else {
            break;
        }
    }

    b_tree::Range::from_prefix(prefix)
}

#[inline]
fn index_range_for<'a, K: Eq + Hash, V>(
    columns: &[K],
    range: &mut HashMap<K, ColumnRange<V>>,
) -> b_tree::Range<V> {
    let mut prefix = Key::with_capacity(range.len());

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
        } else {
            break;
        }
    }

    b_tree::Range::from_prefix(prefix)
}

#[inline]
fn inner_range_for<'a, K, V>(
    query: &IndexQuery<'a, K>,
    range: &HashMap<K, ColumnRange<V>>,
) -> b_tree::Range<V>
where
    K: Eq + Hash + fmt::Debug,
    V: Clone,
{
    let mut inner_range = Key::with_capacity(query.range().len());
    let mut range_columns = query.range().into_iter();

    let inner_range = loop {
        if let Some(col_name) = range_columns.next() {
            match range.get(col_name).cloned().expect("column range") {
                ColumnRange::Eq(value) => inner_range.push(value),
                ColumnRange::In(bounds) => break b_tree::Range::with_bounds(inner_range, bounds),
            }
        } else {
            break b_tree::Range::from(inner_range);
        }
    };

    assert!(range_columns.next().is_none());

    inner_range
}

fn prefix_extractor<K, V>(columns_in: &[K], columns_out: &[K]) -> impl Fn(Key<V>) -> Key<V> + Send
where
    K: PartialEq + fmt::Debug,
    V: Default + Clone,
{
    debug_assert!(columns_out.len() <= columns_in.len());
    debug_assert!(!columns_out.is_empty());
    debug_assert!(
        columns_out.iter().all(|id| columns_in.contains(&id)),
        "{columns_out:?} is not a subset of {columns_in:?}"
    );

    #[cfg(feature = "logging")]
    log::trace!("extract columns {columns_out:?} from {columns_in:?}");

    let indices = columns_out
        .iter()
        .map(|name_out| {
            columns_in
                .iter()
                .position(|name_in| name_in == name_out)
                .expect("column index")
        })
        .collect::<IndexStack<_>>();

    move |mut key| {
        let mut prefix = smallvec![V::default(); indices.len()];

        for (i_to, i_from) in indices.iter().copied().enumerate() {
            mem::swap(&mut key[i_from], &mut prefix[i_to]);
        }

        prefix
    }
}

impl<IS, C, FE> TableState<IS, C, DirWriteGuardOwned<FE>>
where
    IS: IndexSchema + Send + Sync,
    C: Collate<Value = IS::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<IS::Value>> + Send + Sync + 'static,
    DirWriteGuardOwned<FE>: DirDeref<Entry = FE>,
    Node<IS::Value>: FileLoad,
{
    async fn delete_row(&mut self, key: &[IS::Value]) -> Result<bool, io::Error> {
        let row = if let Some(row) = self.get_row(key).await? {
            row
        } else {
            return Ok(false);
        };

        let mut deletes = IndexStack::with_capacity(self.auxiliary.len() + 1);

        for (_name, index) in self.auxiliary.iter_mut() {
            let index_key = borrow_columns(
                &row,
                self.primary.schema().columns(),
                index.schema().columns(),
            );

            deletes.push(async move { index.delete(&index_key).await })
        }

        self.primary.delete(&row).await?;

        for present in try_join_all(deletes).await? {
            assert!(present, "table index is out of sync");
        }

        Ok(true)
    }

    async fn delete_range<'a>(
        &mut self,
        plan: QueryPlan<'a, IS::Id>,
        range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        key_columns: &[IS::Id],
    ) -> Result<usize, io::Error> {
        let mut deleted = 0;

        while let Some(pk) = self.first(&plan, &range, key_columns, key_columns).await? {
            self.delete_row(&pk).await?;
            deleted += 1;
        }

        Ok(deleted)
    }

    async fn delete_all<OG>(&mut self, mut other: TableState<IS, C, OG>) -> Result<(), io::Error>
    where
        OG: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        let mut deletes = IndexStack::with_capacity(self.auxiliary.len() + 1);

        deletes.push(self.primary.delete_all(other.primary));

        for (name, this) in self.auxiliary.iter_mut() {
            let that = other.auxiliary.remove(name).expect("other index");
            deletes.push(this.delete_all(that));
        }

        try_join_all(deletes).await?;

        Ok(())
    }

    async fn merge<OG>(&mut self, mut other: TableState<IS, C, OG>) -> Result<(), io::Error>
    where
        OG: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        let mut merges = IndexStack::with_capacity(self.auxiliary.len() + 1);

        merges.push(self.primary.merge(other.primary));

        for (name, this) in self.auxiliary.iter_mut() {
            let that = other.auxiliary.remove(name).expect("other index");
            merges.push(this.merge(that));
        }

        try_join_all(merges).await?;

        Ok(())
    }

    async fn upsert(&mut self, row: Vec<IS::Value>) -> Result<bool, io::Error> {
        let mut inserts = IndexStack::with_capacity(self.auxiliary.len() + 1);

        for (_name, index) in self.auxiliary.iter_mut() {
            let index_key = clone_columns(
                &row,
                self.primary.schema().columns(),
                index.schema().columns(),
            );

            inserts.push(index.insert(index_key));
        }

        inserts.push(self.primary.insert(row));

        let mut inserts = try_join_all(inserts).await?;
        let new = inserts.pop().expect("insert");
        while let Some(index_new) = inserts.pop() {
            assert_eq!(new, index_new, "index out of sync");
        }

        Ok(new)
    }

    async fn truncate(&mut self) -> Result<(), io::Error> {
        let mut truncates = IndexStack::with_capacity(self.auxiliary.len() + 1);
        truncates.push(self.primary.truncate());

        for index in self.auxiliary.values_mut() {
            truncates.push(index.truncate());
        }

        try_join_all(truncates).await?;

        Ok(())
    }
}

impl<IS, C, FE> TableState<IS, C, DirWriteGuardOwned<FE>> {
    fn downgrade(self) -> TableState<IS, C, Arc<DirReadGuardOwned<FE>>> {
        TableState {
            primary: self.primary.downgrade(),
            auxiliary: self
                .auxiliary
                .into_iter()
                .map(|(name, index)| (name, index.downgrade()))
                .collect(),
        }
    }
}

/// A database table with support for multiple indices
pub struct Table<S, IS, C, G> {
    schema: Arc<TableSchema<S>>,
    state: TableState<IS, C, G>,
}

impl<S, IS, C, G> Clone for Table<S, IS, C, G>
where
    C: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            state: self.state.clone(),
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
            self.state.contains(key).await
        } else {
            Err(bad_key(key, key_len))
        }
    }

    /// Return the first row in the given `range` using the given `order`.
    pub async fn first(
        &self,
        range: Range<S::Id, S::Value>,
        order: &[S::Id],
        select: Option<&[S::Id]>,
    ) -> Result<Option<Row<S::Value>>, io::Error> {
        let range = range.into_inner();
        let select = select.unwrap_or(self.schema.key());
        let plan = self.schema.plan_query(&range, order, self.schema.key())?;

        self.state
            .first(&plan, &range, select, self.schema.key())
            .await
    }

    /// Look up a row by its `key`.
    pub async fn get_row(&self, key: &[S::Value]) -> Result<Option<Row<S::Value>>, io::Error> {
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.state.get_row(&key).await
        } else {
            Err(bad_key(&key, key_len))
        }
    }

    /// Look up a value by its `key`.
    pub async fn get_value(&self, key: &[S::Value]) -> Result<Option<Row<S::Value>>, io::Error> {
        let key_len = self.schema.key().len();

        self.get_row(key)
            .map_ok(move |maybe_row| maybe_row.map(move |mut row| row.drain(key_len..).collect()))
            .await
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
        let range = range.into_inner();
        let plan = self.schema.plan_query(&range, &[], self.schema.key())?;
        self.state.count(plan, range, self.schema.key()).await
    }

    /// Return `true` if the given [`Range`] of this [`Table`] does not contain any rows.
    pub async fn is_empty(&self, range: Range<S::Id, S::Value>) -> Result<bool, io::Error> {
        let range = range.into_inner();
        let plan = self.schema.plan_query(&range, &[], Default::default())?;
        self.state.is_empty(plan, range, self.schema.key()).await
    }

    /// Construct a [`Stream`] of the `select`ed columns of the [`Rows`] within the given `range`.
    pub async fn rows<'a>(
        &'a self,
        range: Range<S::Id, S::Value>,
        order: &'a [S::Id],
        reverse: bool,
        select: Option<&'a [S::Id]>,
    ) -> Result<Rows<S::Value>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::rows with order {order:?}");

        let range = range.into_inner();
        let select = select.unwrap_or(self.schema.primary().columns());
        let plan = self.schema.plan_query(&range, order, self.schema.key())?;

        self.state
            .rows(plan, range, reverse, select, self.schema.key())
            .await
    }

    /// Consume this [`TableReadGuard`] to construct a [`Stream`] of all the rows in the [`Table`].
    pub async fn into_rows(self) -> Result<Rows<S::Value>, io::Error> {
        let rows = self.rows(Range::default(), &[], false, None).await?;
        Ok(Box::pin(rows))
    }
}

impl<S: fmt::Debug, IS, C, G> fmt::Debug for Table<S, IS, C, G> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "table with schema {:?}", self.schema.inner())
    }
}

impl<S, IS, C, FE> Table<S, IS, C, DirWriteGuardOwned<FE>> {
    /// Downgrade this write lock to a read lock.
    pub fn downgrade(self) -> Table<S, IS, C, Arc<DirReadGuardOwned<FE>>> {
        Table {
            schema: self.schema,
            state: self.state.downgrade(),
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
    pub async fn delete_row(&mut self, key: &[S::Value]) -> Result<bool, io::Error> {
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.state.delete_row(key).await
        } else {
            Err(bad_key(&key, key_len))
        }
    }

    /// Delete all rows in the given `range` from this [`Table`].
    pub async fn delete_range(
        &mut self,
        range: Range<S::Id, S::Value>,
    ) -> Result<usize, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::delete_range {range:?}");

        let range = range.into_inner();
        let plan = self.schema.plan_query(&range, &[], self.schema.key())?;

        self.state
            .delete_range(plan, range, self.schema.key())
            .await
    }

    /// Delete all rows from the `other` table from this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn delete_all(
        &mut self,
        other: TableReadGuard<S, S::Index, C, FE>,
    ) -> Result<(), io::Error> {
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

        self.state.delete_all(other.state).await
    }

    /// Insert all rows from the `other` table into this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn merge(
        &mut self,
        other: TableReadGuard<S, S::Index, C, FE>,
    ) -> Result<(), io::Error> {
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

        self.state.merge(other.state).await
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

        let mut row = Vec::with_capacity(key.len() + values.len());
        row.extend(key);
        row.extend(values);

        self.state.upsert(row).map_err(S::Error::from).await
    }

    /// Delete all rows from this [`Table`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::truncate");

        self.state.truncate().await
    }
}

#[inline]
fn borrow_columns<'a, K, V>(row: &'a [V], columns_in: &[K], columns_out: &[K]) -> Key<&'a V>
where
    K: Eq,
{
    assert_eq!(row.len(), columns_in.len());

    debug_assert!(columns_out
        .iter()
        .all(|col_name| columns_in.contains(col_name)));

    columns_out
        .iter()
        .filter_map(|col_name| columns_in.iter().position(|c| c == col_name))
        .map(|i| &row[i])
        .collect()
}

#[inline]
fn clone_columns<K, V>(row: &[V], columns_in: &[K], columns_out: &[K]) -> Vec<V>
where
    K: Eq,
    V: Clone,
{
    assert_eq!(row.len(), columns_in.len());

    debug_assert!(columns_out
        .iter()
        .all(|col_name| columns_in.contains(col_name)));

    columns_out
        .iter()
        .filter_map(|col_name| columns_in.iter().position(|c| c == col_name))
        .map(|i| row[i].clone())
        .collect()
}

#[inline]
fn extract_columns<K, V>(mut row: Key<V>, columns_in: &[K], columns_out: &[K]) -> Key<V>
where
    K: Eq + fmt::Debug,
    V: Default + Clone + fmt::Debug,
{
    assert_eq!(
        row.len(),
        columns_in.len(),
        "row {row:?} does not match column schema {columns_in:?}"
    );

    debug_assert!(
        columns_out
            .iter()
            .all(|col_name| columns_in.contains(col_name)),
        "input columns {columns_in:?} are missing some output columns {columns_out:?}"
    );

    let mut selection = smallvec![V::default(); columns_out.len()];

    for (i_to, name_out) in columns_out.iter().enumerate() {
        let i_from = columns_in
            .iter()
            .position(|name_in| name_in == name_out)
            .expect("column index");

        mem::swap(&mut row[i_from], &mut selection[i_to]);
    }

    selection
}

#[inline]
fn bad_key<V: fmt::Debug>(key: &[V], key_len: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid key: {key:?}, expected exactly {key_len} column(s)",),
    )
}

#[inline]
fn valid_schema<S: Schema>(schema: &S) -> Result<(), io::Error> {
    if schema.primary().columns().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{schema:?} contains no columns"),
        ));
    }

    for (index_name, index) in schema.auxiliary() {
        if index.columns().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("index {index_name} is empty"),
            ));
        }

        for col_name in index.columns() {
            if !schema.primary().columns().contains(col_name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("index {index_name} refers to unknown column {col_name}"),
                ));
            }
        }

        // note: it's inefficient to remove this requirement
        // because it would break the assumption
        // that every key constructed by merging two indices exists in the primary index
        for col_name in schema.key() {
            if !index.columns().contains(col_name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("index {index_name} is missing primary key column {col_name}"),
                ));
            }
        }
    }

    Ok(())
}
