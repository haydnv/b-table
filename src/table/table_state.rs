use std::collections::HashMap;
use std::sync::Arc;
use std::{fmt, io};

use b_tree::{BTree, collate::Collate};
use freqfs::{DirDeref, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::{self, TryFutureExt};
use futures::stream::TryStreamExt;
use safecast::AsType;

use crate::plan::QueryPlan;
use crate::schema::{IndexId, IndexSchema, Range};
use crate::{ColumnRange, IndexStack, Node, Row, Rows};

type KeyStream<'a, V, Id> = (b_tree::Keys<V>, &'a [Id]);

use super::table_utils::{
    borrow_columns, clone_columns, extract_columns, index_range_borrow, index_range_for,
    inner_range_for, prefix_extractor,
};

pub(super) struct TableState<IS, C, G> {
    // IMPORTANT! the auxiliary field must go before primary so that it will be dropped first
    pub(super) auxiliary: HashMap<Arc<str>, BTree<IS, C, G>>,
    pub(super) primary: BTree<IS, C, G>,
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
    pub(super) async fn contains(&self, prefix: &[IS::Value]) -> Result<bool, io::Error> {
        self.primary.contains(prefix).await
    }

    pub(super) async fn get_row(
        &self,
        key: &[IS::Value],
    ) -> Result<Option<Row<IS::Value>>, io::Error> {
        self.primary.first(b_tree::Range::from_prefix(key)).await
    }

    pub(super) async fn first<'a>(
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

            let index_range = index_range_borrow(columns, range);

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
    pub(super) async fn count<'a>(
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

    pub(super) async fn is_empty<'a>(
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
    pub(super) async fn rows<'a>(
        &'a self,
        mut plan: QueryPlan<'a, IS::Id>,
        mut range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        reverse: bool,
        select: &'a [IS::Id],
        key_columns: &'a [IS::Id],
    ) -> Result<Rows<IS::Value>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("construct row stream with plan {plan:?}");

        let mut keys: Option<KeyStream<'a, IS::Value, IS::Id>> = None;

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

            debug_assert!(
                columns_out
                    .iter()
                    .take(columns_in.len())
                    .all(|c| columns_in.contains(c))
            );

            let extract_prefix = prefix_extractor(columns_in, &columns_out[..columns_in.len()]);

            let inner_range = inner_range_for(&query, &range);

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

                debug_assert!(
                    columns_out
                        .iter()
                        .take(columns_in.len())
                        .all(|c| columns_in.contains(c))
                );

                let extract_prefix = prefix_extractor(columns_in, &columns_out[..columns_in.len()]);

                let inner_range = inner_range_for(&query, &range);

                let index = index.clone();

                let index_keys = prefixes
                    .map_ok(extract_prefix)
                    .map_ok(move |prefix| inner_range.clone().prepend(prefix))
                    .map_ok(move |index_range| {
                        let index = index.clone();
                        async move {
                            if reverse {
                                index.keys_rev(index_range).await
                            } else {
                                index.keys(index_range).await
                            }
                        }
                    })
                    .try_buffered(num_cpus::get())
                    .try_flatten();

                keys = Some((Box::pin(index_keys), columns_out))
            } else {
                let index = self.get_index(index_id).expect("index");
                let columns = index.schema().columns();

                let index_range = index_range_for(columns, &mut range);
                assert!(range.is_empty());

                let index_keys = if reverse {
                    index.clone().keys_rev(index_range).await?
                } else {
                    index.clone().keys(index_range).await?
                };
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
            let keys = if reverse {
                self.primary.clone().keys_rev(index_range).await?
            } else {
                self.primary.clone().keys(index_range).await?
            };
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

impl<IS, C, FE> TableState<IS, C, DirWriteGuardOwned<FE>>
where
    IS: IndexSchema + Send + Sync,
    C: Collate<Value = IS::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<IS::Value>> + Send + Sync + 'static,
    DirWriteGuardOwned<FE>: DirDeref<Entry = FE>,
    Node<IS::Value>: FileLoad,
{
    pub(super) async fn delete_row(&mut self, key: &[IS::Value]) -> Result<bool, io::Error> {
        let row = if let Some(row) = self.get_row(key).await? {
            row
        } else {
            return Ok(false);
        };

        let mut deletes = IndexStack::with_capacity(self.auxiliary.len() + 1);

        for index in self.auxiliary.values_mut() {
            let index_key = borrow_columns(
                &row,
                self.primary.schema().columns(),
                index.schema().columns(),
            );

            deletes.push(async move { index.delete(&index_key).await })
        }

        self.primary.delete(&row).await?;

        for present in future::try_join_all(deletes).await? {
            assert!(present, "table index is out of sync");
        }

        Ok(true)
    }

    pub(super) async fn delete_range<'a>(
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

    pub(super) async fn delete_all<OG>(
        &mut self,
        mut other: TableState<IS, C, OG>,
    ) -> Result<(), io::Error>
    where
        OG: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        let mut deletes = IndexStack::with_capacity(self.auxiliary.len() + 1);

        deletes.push(self.primary.delete_all(other.primary));

        for (name, this) in self.auxiliary.iter_mut() {
            let that = other.auxiliary.remove(name).expect("other index");
            deletes.push(this.delete_all(that));
        }

        future::try_join_all(deletes).await?;

        Ok(())
    }

    pub(super) async fn merge<OG>(
        &mut self,
        mut other: TableState<IS, C, OG>,
    ) -> Result<(), io::Error>
    where
        OG: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        let mut merges = IndexStack::with_capacity(self.auxiliary.len() + 1);

        merges.push(self.primary.merge(other.primary));

        for (name, this) in self.auxiliary.iter_mut() {
            let that = other.auxiliary.remove(name).expect("other index");
            merges.push(this.merge(that));
        }

        future::try_join_all(merges).await?;

        Ok(())
    }

    pub(super) async fn upsert(&mut self, row: Vec<IS::Value>) -> Result<bool, io::Error> {
        let mut inserts = IndexStack::with_capacity(self.auxiliary.len() + 1);

        for index in self.auxiliary.values_mut() {
            let index_key = clone_columns(
                &row,
                self.primary.schema().columns(),
                index.schema().columns(),
            );

            inserts.push(index.insert(index_key));
        }

        inserts.push(self.primary.insert(row));

        let mut inserts = future::try_join_all(inserts).await?;
        let new = inserts.pop().expect("insert");
        while let Some(index_new) = inserts.pop() {
            assert_eq!(new, index_new, "index out of sync");
        }

        Ok(new)
    }

    pub(super) async fn truncate(&mut self) -> Result<(), io::Error> {
        let mut truncates = IndexStack::with_capacity(self.auxiliary.len() + 1);
        truncates.push(self.primary.truncate());

        for index in self.auxiliary.values_mut() {
            truncates.push(index.truncate());
        }

        future::try_join_all(truncates).await?;

        Ok(())
    }
}

impl<IS, C, FE> TableState<IS, C, DirWriteGuardOwned<FE>> {
    pub(super) fn downgrade(self) -> TableState<IS, C, Arc<DirReadGuardOwned<FE>>> {
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
