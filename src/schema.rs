use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::ops::Bound;
use std::{fmt, io, iter};

use b_tree::collate::{Collate, Overlap, OverlapsRange, OverlapsValue};

pub use b_tree::{Key, Schema as BTreeSchema};

/// An ID type used to look up a specific table index
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum IndexId<'a> {
    Primary,
    Auxiliary(&'a str),
}

impl<'a> Default for IndexId<'a> {
    fn default() -> Self {
        Self::Primary
    }
}

impl<'a> IndexId<'a> {
    /// Return the name of this index as a [`String`], if this is a named auxiliary index.
    pub fn to_name(&self) -> Option<String> {
        match self {
            Self::Primary => None,
            Self::Auxiliary(index_id) => Some(index_id.to_string()),
        }
    }
}

impl<'a> From<&'a str> for IndexId<'a> {
    fn from(id: &'a str) -> Self {
        Self::Auxiliary(id)
    }
}

impl<'a> From<&'a String> for IndexId<'a> {
    fn from(id: &'a String) -> Self {
        Self::Auxiliary(id)
    }
}

impl<'a> From<&'a Option<String>> for IndexId<'a> {
    fn from(id: &'a Option<String>) -> Self {
        match id {
            None => Self::Primary,
            Some(index_id) => Self::Auxiliary(index_id),
        }
    }
}

impl<'a> fmt::Debug for IndexId<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Primary => f.write_str("primary"),
            Self::Auxiliary(id) => write!(f, "{}", id),
        }
    }
}

#[derive(Eq, PartialEq)]
pub struct QueryPlan<'a, S> {
    schema: &'a S,
    pub indices: VecDeque<IndexId<'a>>,
}

impl<'a, S> Clone for QueryPlan<'a, S> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema,
            indices: self.indices.clone(),
        }
    }
}

impl<'a, S: Schema> QueryPlan<'a, S> {
    fn default(schema: &'a S) -> Self {
        Self {
            schema,
            indices: VecDeque::default(),
        }
    }

    fn with_index<Id>(
        schema: &'a S,
        order: &[S::Id],
        range: &Range<S::Id, S::Value>,
        index_id: Id,
    ) -> Option<Self>
    where
        IndexId<'a>: From<Id>,
    {
        let index_id = index_id.into();
        let index = get_index(schema, index_id);

        if index.columns().is_empty() {
            None
        } else if order.is_empty() {
            if range.is_default() {
                Some(Self {
                    schema,
                    indices: [index_id].into_iter().collect(),
                })
            } else if range.columns.contains_key(&index.columns()[0]) {
                let mut indices = VecDeque::with_capacity((schema.auxiliary().len() + 1) * 2);
                indices.push_back(index_id);
                Some(Self { schema, indices })
            } else {
                None
            }
        } else if index.columns()[0] == order[0] {
            let mut indices = VecDeque::with_capacity((schema.auxiliary().len() + 1) * 2);
            indices.push_back(index_id);
            Some(Self { schema, indices })
        } else {
            None
        }
    }

    fn clone_and_push(&self, index_id: IndexId<'a>) -> Self {
        let mut clone = self.clone();
        clone.indices.push_back(index_id);
        clone
    }

    fn covers(
        &self,
        order: &[S::Id],
        range: &Range<S::Id, S::Value>,
    ) -> (usize, HashSet<&'a S::Id>) {
        let mut covered_order = 0;
        let mut covered_range = HashSet::with_capacity(range.len());

        for index_id in self.indices.iter().copied() {
            let index = get_index(self.schema, index_id);
            let mut index_covers = 0;

            if covered_order < order.len() {
                for col_name in index.columns() {
                    if covered_order + index_covers < order.len() {
                        if &order[covered_order + index_covers] == col_name {
                            index_covers += 1;

                            if let Some(col_range) = range.get(col_name) {
                                covered_range.insert(col_name);

                                if col_range.is_range() {
                                    break;
                                }
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            } else {
                for col_name in index.columns() {
                    if let Some(col_range) = range.get(col_name) {
                        covered_range.insert(col_name);

                        if col_range.is_range() {
                            break;
                        }
                    }
                }
            }

            covered_order += index_covers;
        }

        (covered_order, covered_range)
    }

    fn is_complete(&self, order: &[S::Id], range: &Range<S::Id, S::Value>) -> bool {
        let (covered_order, covered_range) = self.covers(order, range);
        debug_assert!(covered_order <= order.len());
        debug_assert!(covered_range.len() <= range.len());
        covered_order == order.len() && covered_range.len() == range.len()
    }

    fn needs<Id>(&self, order: &[S::Id], range: &Range<S::Id, S::Value>, index_id: Id) -> bool
    where
        IndexId<'a>: From<Id>,
    {
        let (covered_order, covered_range) = self.covers(order, range);
        debug_assert!(covered_order <= order.len());

        let index_columns = get_index(self.schema, index_id.into()).columns();
        assert!(!index_columns.is_empty());

        if covered_order < order.len() {
            index_columns.starts_with(&order[covered_order..covered_order + 1])
        } else {
            range.columns.contains_key(&index_columns[0])
                && !covered_range.contains(&index_columns[0])
        }
    }

    fn supports<Id>(&'a self, index_id: Id) -> bool
    where
        IndexId<'a>: From<Id>,
    {
        if self.indices.is_empty() {
            true
        } else {
            let column_name = &get_index(self.schema, index_id.into()).columns()[0];

            for index_id in &self.indices {
                let index = get_index(self.schema, *index_id);
                if index.columns().contains(column_name) {
                    return true;
                }
            }

            false
        }
    }
}

impl<'a, S: fmt::Debug> fmt::Debug for QueryPlan<'a, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "plan to query indices {:?} of {:?}",
            self.indices, self.schema
        )
    }
}

/// The schema of a table index
pub trait IndexSchema: BTreeSchema + Clone {
    type Id: Hash + Eq + Clone + fmt::Debug + fmt::Display;

    /// Borrow the list of columns specified by this schema.
    fn columns(&self) -> &[Self::Id];

    /// Construct a [`Key`] for this index from the values of a [`Key`] from a different index.
    ///
    /// Panics:
    ///     - if `other_key` does not match `other_schema`
    ///     - if `other_schema` does not contain all the columns of this schema
    fn extract_key(&self, other_key: &[Self::Value], other_schema: &Self) -> Key<Self::Value> {
        assert_eq!(other_key.len(), BTreeSchema::len(other_schema));

        let mut key = Key::with_capacity(BTreeSchema::len(self));
        for this_col_name in self.columns() {
            let mut found = false;
            for (that_col_name, value) in other_schema.columns().iter().zip(other_key) {
                if this_col_name == that_col_name {
                    key.push(value.clone());
                    found = true;
                    break;
                }
            }

            if !found {
                panic!("index {other_schema:?} is missing column {this_col_name}");
            }
        }

        key
    }

    /// Return `true` if an index with this schema supports the given [`Range`].
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
pub trait Schema: Eq + Sized + fmt::Debug {
    type Id: Hash + Eq + Clone + Send + Sync + fmt::Debug + fmt::Display + 'static;
    type Error: std::error::Error + From<io::Error>;
    type Value: Clone + Eq + Send + Sync + fmt::Debug + 'static;
    type Index: IndexSchema<Error = Self::Error, Id = Self::Id, Value = Self::Value>
        + Send
        + Sync
        + 'static;

    /// Borrow the names of the columns in the primary key.
    fn key(&self) -> &[Self::Id];

    /// Borrow the names of the value columns.
    fn values(&self) -> &[Self::Id];

    /// Borrow the schema of the primary index.
    fn primary(&self) -> &Self::Index;

    /// Borrow the schemata of the auxiliary indices.
    /// This is ordered so that the first index which matches a given [`Range`] will be used.
    fn auxiliary(&self) -> &[(String, Self::Index)];

    /// Compute a sequence of indices to read from in order construct a result set
    /// with the given range and order.
    fn plan_query<'a>(
        &'a self,
        order: &[Self::Id],
        range: &Range<Self::Id, Self::Value>,
    ) -> Result<QueryPlan<'a, Self>, io::Error> {
        if self.primary().columns().starts_with(order) && self.primary().supports(range) {
            return Ok(QueryPlan::default(self));
        } else if self.auxiliary().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this table has no auxiliary indices to support order and range queries",
            ));
        }

        let mut candidates = VecDeque::with_capacity(self.auxiliary().len() * 2);

        for (name, _index) in self.auxiliary() {
            if let Some(candidate) = QueryPlan::with_index(self, order, range, name) {
                if candidate.is_complete(order, range) {
                    return Ok(candidate);
                } else {
                    candidates.push_back(candidate);
                }
            }
        }

        while let Some(plan) = candidates.pop_front() {
            let indices = iter::once((IndexId::Primary, self.primary())).chain(
                self.auxiliary()
                    .iter()
                    .map(|(name, index)| (IndexId::from(name), index)),
            );

            for (name, _index) in indices {
                if plan.supports(name) {
                    if plan.needs(order, range, name) {
                        let candidate = plan.clone_and_push(name);

                        if candidate.is_complete(order, range) {
                            return Ok(candidate);
                        } else {
                            candidates.push_back(candidate);
                        }
                    } else {
                        #[cfg(feature = "logging")]
                        log::trace!("{plan:?} does not need index {name:?}");
                    }
                } else {
                    #[cfg(feature = "logging")]
                    log::trace!("{plan:?} does not support index {name:?}");
                }
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no index supports range {range:?} with order {order:?}"),
        ))
    }

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

impl<V> ColumnRange<V> {
    fn is_range(&self) -> bool {
        match self {
            Self::Eq(_) => false,
            Self::In(_) => true,
        }
    }
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

impl<K, V> From<HashMap<K, ColumnRange<V>>> for Range<K, V> {
    fn from(columns: HashMap<K, ColumnRange<V>>) -> Self {
        Self { columns }
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

impl<K: Hash + Eq, V> FromIterator<(K, ColumnRange<V>)> for Range<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, ColumnRange<V>)>>(iter: I) -> Self {
        Self {
            columns: iter.into_iter().collect(),
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

        for (i, (column, bound)) in self.columns.iter().enumerate() {
            write!(f, "{column}: {bound:?}")?;

            if i < self.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str("}")
    }
}

fn get_index<'a, S: Schema>(schema: &'a S, index_id: IndexId<'a>) -> &'a S::Index {
    match index_id.into() {
        IndexId::Primary => schema.primary(),
        IndexId::Auxiliary(index_id) => schema
            .auxiliary()
            .iter()
            .filter_map(|(name, index)| if name == index_id { Some(index) } else { None })
            .next()
            .expect("index"),
    }
}
