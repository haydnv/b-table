use std::collections::HashMap;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io, iter};

use freqfs::{Dir, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::{join_all, try_join_all};
use futures::stream::{Stream, TryStreamExt};
use safecast::AsType;

use super::index::collate::Collate;
use super::index::{Index, IndexLock, Key};
use super::schema::*;
use super::Node;

const PRIMARY: &str = "primary";

/// A read guard acquired on a [`TableLock`]
pub type TableReadGuard<S, IS, C, FE> = Table<S, IS, C, DirReadGuardOwned<FE>>;

/// A write guard acquired on a [`TableLock`]
pub type TableWriteGuard<S, IS, C, FE> = Table<S, IS, C, DirWriteGuardOwned<FE>>;

/// A futures-aware read-write lock on a [`Table`]
pub struct TableLock<S, IS, C, FE> {
    schema: Arc<S>,
    dir: DirLock<FE>,
    primary: IndexLock<IS, C, FE>,
    auxiliary: HashMap<String, IndexLock<IS, C, FE>>,
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
    pub fn collator(&self) -> &Arc<super::index::Collator<C>> {
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

        let mut auxiliary = HashMap::with_capacity(schema.auxiliary().len());
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

        let mut auxiliary = HashMap::with_capacity(schema.auxiliary().len());
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
        // lock the primary key first, separately from the indices, to avoid a deadlock
        Table {
            schema: self.schema.clone(),
            primary: self.primary.read().await,
            auxiliary: join_all(self.auxiliary.values().map(|index| index.read())).await,
        }
    }

    /// Lock this [`Table`] for reading, without borrowing.
    pub async fn into_read(self) -> TableReadGuard<S, S::Index, C, FE> {
        // lock the primary key first, separately from the indices, to avoid a deadlock
        Table {
            schema: self.schema,
            primary: self.primary.into_read().await,
            auxiliary: join_all(self.auxiliary.into_values().map(|index| index.into_read())).await,
        }
    }

    /// Lock this [`Table`] for writing.
    pub async fn write(&self) -> TableWriteGuard<S, S::Index, C, FE> {
        // lock the primary key first, separately from the indices, to avoid a deadlock
        Table {
            schema: self.schema.clone(),
            primary: self.primary.write().await,
            auxiliary: join_all(self.auxiliary.values().map(|index| index.write())).await,
        }
    }

    /// Lock this [`Table`] for writing, without borrowing.
    pub async fn into_write(self) -> TableWriteGuard<S, S::Index, C, FE> {
        // lock the primary key first, separately from the indices, to avoid a deadlock
        Table {
            schema: self.schema,
            primary: self.primary.into_write().await,
            auxiliary: join_all(self.auxiliary.into_values().map(|index| index.into_write())).await,
        }
    }
}

impl<S, C, FE> TableLock<S, S::Index, C, FE>
where
    S: Schema + Send + Sync + 'static,
    C: Collate<Value = S::Value> + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    S::Index: Send + Sync,
    S::Value: Send,
    Node<S::Value>: FileLoad,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Construct a [`Stream`] of the values of the `columns` of the rows within the given `range`.
    pub async fn rows(
        self,
        range: Range<S::Id, S::Value>,
        order: &[S::Id],
        reverse: bool,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<S::Value>, io::Error>> + Send>>, io::Error>
    {
        // TODO: formulate a query plan then follow it to stitch together multiple indices

        #[cfg(feature = "logging")]
        log::debug!("Table::rows");

        if self.primary.schema().supports(&range)
            && self.primary.schema().columns().starts_with(order)
        {
            #[cfg(feature = "logging")]
            log::trace!("Table::rows will read only from the primary index");

            let range = self.primary.schema().extract_range(range).expect("range");
            let index = self.primary.read().await;

            #[cfg(feature = "logging")]
            log::trace!("Table::rows got a read lock on the primary index");

            let stream = index.keys(range, reverse);
            return Ok(Box::pin(stream));
        }

        #[allow(unused_variables)]
        for (name, index) in self.auxiliary {
            if index.schema().supports(&range) && index.schema().columns().starts_with(order) {
                #[cfg(feature = "logging")]
                log::trace!("Table::rows will read only from index {}", name);

                let range = index.schema().extract_range(range).expect("range");

                let schema = self.schema;
                let primary = self.primary;
                let index_schema = index.schema().clone();
                let index = index.read().await;
                let stream = index
                    .keys(range, reverse)
                    .map_ok(move |index_key| {
                        let key = schema.extract_key(index_key, &index_schema);
                        let range = b_tree::Range::from_prefix(key);
                        let primary = primary.clone();

                        async move {
                            let index = primary.read().await;
                            Ok(index.keys(range, reverse))
                        }
                    })
                    .try_buffered(num_cpus::get())
                    .try_flatten();

                return Ok(Box::pin(stream));
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "this table has no index which supports the requested range: {:?}",
                range
            ),
        ))
    }
}

/// A database table with support for multiple indices
pub struct Table<S, IS, C, G> {
    schema: Arc<S>,
    primary: Index<IS, C, G>,
    auxiliary: Vec<Index<IS, C, G>>,
}

impl<S, C, FE, G> Table<S, S::Index, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: Deref<Target = Dir<FE>> + 'static,
    Node<S::Value>: FileLoad,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Count how many rows in this [`Table`] lie within the given `range`.
    pub async fn count(&self, range: Range<S::Id, S::Value>) -> Result<u64, io::Error> {
        // TODO: formulate a query plan then follow it to stitch together multiple indices

        for index in iter::once(&self.primary).chain(self.auxiliary.iter()) {
            if index.schema().supports(&range) {
                let range = index.schema().extract_range(range).expect("range");
                return index.count(&range).await;
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "this table has no index to support the requested range: {:?}",
                range
            ),
        ))
    }

    /// Return `true` if the given `key` is present in this [`Table`].
    pub async fn contains(&self, key: &Key<S::Value>) -> Result<bool, io::Error> {
        self.primary.contains(key).await
    }

    /// Look up a row by its `key`.
    pub async fn get(&self, key: Key<S::Value>) -> Result<Option<Vec<S::Value>>, io::Error> {
        self.primary.first(&b_tree::Range::from_prefix(key)).await
    }

    /// Return `true` if the given [`Range`] of this [`Table`] does not contain any rows.
    pub async fn is_empty(&self, range: Range<S::Id, S::Value>) -> Result<bool, io::Error> {
        for index in iter::once(&self.primary).chain(self.auxiliary.iter()) {
            if index.schema().supports(&range) {
                let range = index.schema().extract_range(range).expect("range");
                return index.is_empty(&range).await;
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "this table has no index to support the requested range: {:?}",
                range
            ),
        ))
    }
}

impl<S, IS, C, FE> Table<S, IS, C, DirWriteGuardOwned<FE>> {
    /// Downgrade this write lock to a read lock.
    pub fn downgrade(self) -> Table<S, IS, C, DirReadGuardOwned<FE>> {
        Table {
            schema: self.schema,
            primary: self.primary.downgrade(),
            auxiliary: self
                .auxiliary
                .into_iter()
                .map(|index| index.downgrade())
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
        let row = if let Some(row) = self.get(key).await? {
            row
        } else {
            return Ok(false);
        };

        let mut deletes = Vec::with_capacity(self.auxiliary.len() + 1);

        for index in &mut self.auxiliary {
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
    pub async fn delete_range(&mut self, range: Range<S::Id, S::Value>) -> Result<(), S::Error> {
        // TODO: formulate a query plan then follow it to stitch together multiple indices

        let index = self.choose_index(&range)?;

        let range = match index {
            None => self.primary.schema().extract_range(range),
            Some(i) => self.auxiliary[i].schema().extract_range(range),
        }
        .expect("index range");

        while let Some(key) = self.next_row(index, &range).await? {
            self.delete_row(key).await?;
        }

        Ok(())
    }

    // TODO: delete
    fn choose_index(&self, range: &Range<S::Id, S::Value>) -> Result<Option<usize>, io::Error> {
        if self.primary.schema().supports(&range) {
            Ok(None)
        } else {
            for (i, index) in self.auxiliary.iter().enumerate() {
                if index.schema().supports(&range) {
                    return Ok(Some(i));
                }
            }

            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("no index supports the range {range:?}"),
            ))
        }
    }

    // TODO: delete
    async fn next_row(
        &self,
        index: Option<usize>,
        range: &b_tree::Range<S::Value>,
    ) -> Result<Option<b_tree::Key<S::Value>>, io::Error> {
        let index = match index {
            None => &self.primary,
            Some(i) => &self.auxiliary[i],
        };

        if let Some(row) = index.first(range).await? {
            let key = self.primary.schema().extract_key(&row, index.schema());
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    /// Delete all rows from the `other` table from this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn delete_all(
        &mut self,
        other: TableReadGuard<S, S::Index, C, FE>,
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

        for (this, that) in self.auxiliary.iter_mut().zip(other.auxiliary) {
            deletes.push(this.delete_all(that));
        }

        try_join_all(deletes).await?;

        Ok(())
    }

    /// Insert all rows from the `other` table into this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn merge(
        &mut self,
        other: TableReadGuard<S, S::Index, C, FE>,
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

        for (this, that) in self.auxiliary.iter_mut().zip(other.auxiliary) {
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

    /// Delete all rows from this [`Table`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        let mut truncates = Vec::with_capacity(self.auxiliary.len() + 1);
        truncates.push(self.primary.truncate());

        for index in &mut self.auxiliary {
            truncates.push(index.truncate());
        }

        try_join_all(truncates).await?;

        Ok(())
    }
}
