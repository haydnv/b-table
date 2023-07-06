use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io};

use freqfs::{DirDeref, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::{try_join_all, TryFutureExt};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use safecast::AsType;

use super::index::collate::Collate;
use super::index::{self, Index, IndexLock, Key};
use super::schema::*;
use super::Node;

const PRIMARY: &str = "primary";

/// A read guard acquired on a [`TableLock`]
pub type TableReadGuard<S, IS, C, FE> = Table<S, IS, C, Arc<DirReadGuardOwned<FE>>>;

/// A write guard acquired on a [`TableLock`]
pub type TableWriteGuard<S, IS, C, FE> = Table<S, IS, C, DirWriteGuardOwned<FE>>;

/// A stream of table rows
pub type Rows<V> = Pin<Box<dyn Stream<Item = Result<Vec<V>, io::Error>> + Send>>;

/// A futures-aware read-write lock on a [`Table`]
pub struct TableLock<S, IS, C, FE> {
    schema: Arc<S>,
    dir: DirLock<FE>,
    primary: IndexLock<IS, C, FE>,
    auxiliary: BTreeMap<String, IndexLock<IS, C, FE>>,
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
    /// Borrow the [`Schema`] of this [`Table`].
    pub fn schema(&self) -> &S {
        &self.schema
    }

    /// Borrow the collator for this [`Table`].
    pub fn collator(&self) -> &Arc<index::Collator<C>> {
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
            IndexLock::create(schema.primary().clone(), collator.clone(), dir)
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.create_dir(name.to_string())?;
                IndexLock::create(schema.clone(), collator.clone(), dir)
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
        for (_name, index) in schema.auxiliary() {
            if !index.columns().ends_with(schema.key()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "index columns must end with the primary key of the table",
                ));
            }
        }

        let mut dir_contents = dir.try_write()?;

        let primary = {
            let dir = dir_contents.get_or_create_dir(PRIMARY.to_string())?;
            IndexLock::load(schema.primary().clone(), collator.clone(), dir.clone())
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.get_or_create_dir(name.clone())?;
                IndexLock::load(schema.clone(), collator.clone(), dir.clone())
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

impl<S: Schema, C, FE: Send + Sync> TableLock<S, S::Index, C, FE>
where
    Node<S::Value>: FileLoad,
{
    /// Lock this [`Table`] for reading.
    pub async fn read(&self) -> TableReadGuard<S, S::Index, C, FE> {
        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.read().await;

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in &self.auxiliary {
            let index = index.read().await;
            auxiliary.insert(name.clone(), index);
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }

    /// Lock this [`Table`] for reading, without borrowing.
    pub async fn into_read(self) -> TableReadGuard<S, S::Index, C, FE> {
        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.into_read().await;

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary {
            let index = index.into_read().await;
            auxiliary.insert(name, index);
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }

    /// Lock this [`Table`] for writing.
    pub async fn write(&self) -> TableWriteGuard<S, S::Index, C, FE> {
        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.write().await;

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in &self.auxiliary {
            let index = index.write().await;
            auxiliary.insert(name.clone(), index);
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }

    /// Lock this [`Table`] for writing, without borrowing.
    pub async fn into_write(self) -> TableWriteGuard<S, S::Index, C, FE> {
        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.into_write().await;

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary {
            let index = index.into_write().await;
            auxiliary.insert(name, index);
        }

        Table {
            schema,
            primary,
            auxiliary,
        }
    }
}

/// A database table with support for multiple indices
pub struct Table<S, IS, C, G> {
    schema: Arc<S>,
    primary: Index<IS, C, G>,
    auxiliary: HashMap<String, Index<IS, C, G>>, // TODO: should this be in an Arc?
}

impl<S, IS, C, G> Clone for Table<S, IS, C, G>
where
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
    pub async fn contains(&self, key: &Key<S::Value>) -> Result<bool, io::Error> {
        self.primary.contains(key).await
    }

    /// Look up a row by its `key`.
    pub async fn get_row(&self, key: Key<S::Value>) -> Result<Option<Vec<S::Value>>, S::Error> {
        let key = self.schema.validate_key(key)?;

        self.primary
            .first(&b_tree::Range::from_prefix(key))
            .map_err(S::Error::from)
            .await
    }

    /// Look up a value by its `key`.
    pub async fn get_value(&self, key: Key<S::Value>) -> Result<Option<Vec<S::Value>>, S::Error> {
        let key = self.schema.validate_key(key)?;
        let key_len = self.schema.key().len();

        self.primary
            .first(&b_tree::Range::from_prefix(key))
            .map_ok(|maybe_row| maybe_row.map(|row| row[key_len..].to_vec()))
            .map_err(S::Error::from)
            .await
    }
}

impl<S, C, FE, G> Table<S, S::Index, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<S::Value>: FileLoad,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Count how many rows in this [`Table`] lie within the given `range`.
    pub async fn count(&self, range: Range<S::Id, S::Value>) -> Result<u64, io::Error> {
        if range.is_default() {
            self.primary.count(&index::Range::default()).await
        } else {
            // TODO: optimize & avoid the need to move self
            let mut rows = self.rows(range, &[], false, None)?;

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
            self.primary.is_empty(&index::Range::default()).await
        } else {
            // TODO: optimize & avoid the need to move self
            let mut rows = self.rows(range, &[], false, None)?;
            rows.try_next()
                .map_ok(|maybe_row| maybe_row.is_none())
                .await
        }
    }

    /// Construct a [`Stream`] of the values of the `selected` columns within the given `range`.
    pub fn rows(
        &self,
        range: Range<S::Id, S::Value>,
        order: &[S::Id],
        reverse: bool,
        selected: Option<&[S::Id]>,
    ) -> Result<Rows<S::Value>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::rows");

        let mut plan = self.schema.plan_query(order, &range)?;

        let global_columns = selected.unwrap_or_else(|| self.schema.primary().columns());
        let mut global_order = order;
        let mut global_range = range.into_inner();

        let mut local_keys: Option<Rows<S::Value>> = None;
        let mut local_columns: Option<&[S::Id]> = None;

        // for each index in the plan:
        //   construct the index order and range
        //   if there are still global order and range constraints unhandled:
        //     (merge the previous index and) group the index by the order+range columns
        //     reset the local column selection
        //   else:
        //     (merge the previous index and) select the keys from the index
        //     reset the local column selection

        if let Some(index_id) = plan.indices.pop_front() {
            let index = self.auxiliary.get(index_id).expect("index");
            let index_order = index
                .schema()
                .columns()
                .iter()
                .zip(global_order)
                .take_while(|(ic, oc)| ic == oc)
                .count();

            let index_range = index_range_for(index.schema().columns(), &mut global_range);

            global_order = &global_order[index_order..];

            if plan.indices.is_empty() {
                local_keys = Some(Box::pin(index.clone().keys(index_range, reverse)));
                local_columns = Some(index.schema().columns());
            } else {
                let columns = &global_order[..index_order];
                let groups = index.clone().group_by(index_range, columns, reverse)?;

                local_keys = Some(Box::pin(groups));
                local_columns = Some(columns);
            }
        }

        while let Some(index_id) = plan.indices.pop_front() {
            let index = self.auxiliary.get(index_id).expect("index");
            let keys = local_keys.take().expect("keys");
            let columns = local_columns.expect("local columns").to_vec();

            let column_range = if columns.len() < index.schema().columns().len() {
                global_range.remove(columns.last().expect("range column"))
            } else {
                None
            };

            let index_columns = index.schema().columns().to_vec();
            let index_order = index_columns
                .iter()
                .zip(global_order)
                .take_while(|(ic, oc)| ic == oc)
                .count();

            let index = index.clone();
            let merge_source = keys.map_ok(move |key| {
                let mut prefix = Vec::<S::Value>::with_capacity(index_columns.len());

                // TODO: would it be more efficient to use a HashMap?
                for index_col_name in &index_columns {
                    for (key_col_name, value) in columns.iter().zip(&key) {
                        if index_col_name == key_col_name {
                            prefix.push(value.clone());
                        }
                    }
                }

                if let Some(column_range) = column_range.clone() {
                    match column_range {
                        ColumnRange::Eq(value) => {
                            prefix.push(value);
                            index::Range::from_prefix(prefix)
                        }
                        ColumnRange::In(bounds) => index::Range::with_bounds(prefix, bounds),
                    }
                } else {
                    index::Range::from_prefix(prefix)
                }
            });

            local_keys = if plan.indices.is_empty() {
                local_columns = self
                    .auxiliary
                    .get(index_id)
                    .map(|index| index.schema().columns());

                global_order = &global_order[index_order..];

                let keys = merge_source
                    .map(move |result| result.map(|range| index.clone().keys(range, reverse)))
                    .try_flatten();

                Some(Box::pin(keys))
            } else {
                local_columns = Some(&global_order[..index_order]);
                let columns = global_order[..index_order].to_vec();
                global_order = &global_order[index_order..];

                let keys = merge_source
                    .map(move |result| {
                        result.and_then(|range| index.clone().group_by(range, &columns, reverse))
                    })
                    .try_flatten();

                Some(Box::pin(keys))
            }
        }

        // if the local column selection includes the global column selection:
        //   return the index key stream with the global column selection
        // else:
        //   for each selected key in the index, construct a range of the primary index
        //   select a stream of that range
        //   flatten the streams

        if let Some(local_columns) = local_columns {
            let local_keys = local_keys.expect("keys");

            let selected_columns_present = global_columns
                .iter()
                .all(|col_name| local_columns.contains(col_name));

            if selected_columns_present {
                let local_columns = local_columns.to_vec();
                let global_columns = global_columns.to_vec();
                let rows = local_keys.map_ok(move |key| {
                    let mut row = Vec::with_capacity(key.len());

                    // TODO: would it be more efficient to use a HashMap?
                    for col_name in &global_columns {
                        for (key_col_name, value) in local_columns.iter().zip(&key) {
                            if col_name == key_col_name {
                                row.push(value.clone());
                                break;
                            }
                        }
                    }

                    row
                });

                Ok(Box::pin(rows))
            } else {
                let primary = self.primary.clone();
                let pk_columns = self.schema.key().to_vec();
                assert!(pk_columns
                    .iter()
                    .all(|col_name| local_columns.contains(col_name)));

                let local_columns = local_columns.to_vec();
                let rows = local_keys
                    .map_ok(move |key| {
                        let mut pk = Vec::with_capacity(pk_columns.len());

                        for col_name in &pk_columns {
                            for (key_col_name, value) in local_columns.iter().zip(&key) {
                                if col_name == key_col_name {
                                    pk.push(value.clone());
                                    break;
                                }
                            }
                        }

                        pk
                    })
                    .map(move |result| {
                        let primary = primary.clone();

                        result.map(move |pk| async move {
                            let pk = index::Range::from_prefix(pk);
                            let row = primary.first(&pk).await?;
                            Ok(row.expect("row"))
                        })
                    })
                    .try_buffered(num_cpus::get());

                Ok(Box::pin(rows))
            }
        } else {
            assert!(self.primary.schema().columns().starts_with(global_order));
            let range = index_range_for(self.primary.schema().columns(), &mut global_range);
            assert!(global_range.is_empty());
            let rows = self.primary.clone().keys(range, reverse);
            Ok(Box::pin(rows))
        }
    }
}

#[inline]
fn index_range_for<K: Eq + Hash, V>(
    columns: &[K],
    range: &mut HashMap<K, ColumnRange<V>>,
) -> index::Range<V> {
    let mut prefix = Vec::with_capacity(range.len());

    for col_name in columns {
        if let Some(col_range) = range.remove(col_name) {
            match col_range {
                ColumnRange::Eq(value) => {
                    prefix.push(value);
                }
                ColumnRange::In(bounds) => {
                    return index::Range::with_bounds(prefix, bounds);
                }
            }
        }
    }

    index::Range::from_prefix(prefix)
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
    C: Collate<Value = S::Value> + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    <S as Schema>::Index: Send + Sync,
    Node<S::Value>: FileLoad + fmt::Debug,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Delete a row from this [`Table`] by its `key`.
    /// Returns `true` if the given `key` was present.
    pub async fn delete_row(&mut self, key: Key<S::Value>) -> Result<bool, S::Error> {
        let row = if let Some(row) = self.get_row(key).await? {
            row
        } else {
            return Ok(false);
        };

        let mut deletes = Vec::with_capacity(self.auxiliary.len() + 1);

        for index in self.auxiliary.values_mut() {
            deletes.push(async {
                let row = IndexSchema::extract_key(self.schema.primary(), &row, index.schema());
                index.delete(&row).await
            })
        }

        self.primary.delete(&row).await?;

        for present in try_join_all(deletes).await? {
            assert!(present, "table index is out of sync");
        }

        Ok(true)
    }

    /// Delete all rows in the given `range` from this [`Table`].
    pub async fn delete_range(&mut self, _range: Range<S::Id, S::Value>) -> Result<(), S::Error> {
        // TODO: formulate a query plan then follow it to stitch together multiple indices
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
                    other.schema, self.schema
                ),
            )
            .into());
        }

        let mut deletes = Vec::with_capacity(self.auxiliary.len() + 1);

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
                    other.schema, self.schema
                ),
            )
            .into());
        }

        let mut merges = Vec::with_capacity(self.auxiliary.len() + 1);

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
        let key = self.schema.validate_key(key)?;
        let values = self.schema.validate_values(values)?;

        let mut row = key;
        row.extend(values);

        let mut inserts = Vec::with_capacity(self.auxiliary.len() + 1);

        for index in self.auxiliary.values_mut() {
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

    /// Delete all rows from this [`Table`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        let mut truncates = Vec::with_capacity(self.auxiliary.len() + 1);
        truncates.push(self.primary.truncate());

        for index in self.auxiliary.values_mut() {
            truncates.push(index.truncate());
        }

        try_join_all(truncates).await?;

        Ok(())
    }
}
