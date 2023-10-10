use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::ops::{Bound, Deref};
use std::{fmt, io, iter};

use b_tree::collate::*;
use smallvec::SmallVec;

use crate::table::ROW_STACK_SIZE;
use crate::IndexStack;

pub use b_tree::Schema as BTreeSchema;

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

impl<'a> fmt::Debug for IndexId<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Primary => f.write_str("primary"),
            Self::Auxiliary(id) => id.fmt(f),
        }
    }
}

#[derive(Clone)]
struct IndexQuery<'a, K, V> {
    range: Range<K, V>,
    order: &'a [K],
    select: usize,
}

impl<'a, K, V> IndexQuery<'a, K, V> {
    fn with_constraints(range: &mut Range<K, V>, order: &'a [K]) -> Option<Self> {
        todo!()
    }
}

/// The schema of a table index
pub trait IndexSchema: BTreeSchema + Clone {
    type Id: Hash + Eq + Clone + fmt::Debug + fmt::Display;

    /// Borrow the list of columns specified by this schema.
    fn columns(&self) -> &[Self::Id];

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
    /// The type of human-readable identifier used by columns and indices in this [`Schema`]
    type Id: Hash + Eq + Clone + Send + Sync + fmt::Debug + fmt::Display + 'static;

    /// The type of validation error used by this [`Schema`]
    type Error: std::error::Error + From<io::Error>;

    /// The type of column value used by a [`Table`] with this [`Schema`]
    type Value: Clone + Eq + Send + Sync + fmt::Debug + 'static;

    /// The type of schema used by the indices which compose a [`Table`] with this [`Schema`]
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

#[derive(Clone)]
pub(crate) struct QueryPlan<'a, K, V> {
    indices: IndexStack<(IndexId<'a>, IndexQuery<'a, K, V>)>,
}

impl<'a, K, V> Default for QueryPlan<'a, K, V> {
    fn default() -> Self {
        Self {
            indices: IndexStack::default(),
        }
    }
}

impl<'a, K: Clone, V: Clone> QueryPlan<'a, K, V> {
    pub fn new<S>(
        schema: &'a TableSchema<S>,
        mut range: Range<K, V>,
        order: &'a [K],
    ) -> Option<Self>
    where
        S: Schema<Id = K, Value = V>,
    {
        let candidate = Self::default();

        if candidate.supports(schema, &range, order) {
            return Some(candidate);
        }

        let mut candidates = VecDeque::with_capacity((schema.auxiliary().len() + 1) * 2);
        candidates.push_back(candidate);

        while let Some(candidate) = candidates.pop_front() {
            for (index_id, index) in schema.indices() {
                // check whether this candidate needs the given index
                // if so, create a new candidate with this index name and add it to the queue
                if let Some(query) = candidate.needs::<S>(index, &mut range, order) {
                    let candidate = candidate.clone_and_push(index_id, query);

                    if candidate.supports(schema, &range, order) {
                        return Some(candidate);
                    } else {
                        candidates.push_back(candidate);
                    }
                }
            }
        }

        None
    }

    fn clone_and_push(&self, index_id: IndexId<'a>, query: IndexQuery<'a, K, V>) -> Self {
        let mut clone = self.clone();
        clone.indices.push((index_id, query));
        clone
    }

    fn needs<S: Schema>(
        &self,
        index: &'a S::Index,
        range: &mut Range<S::Id, S::Value>,
        order: &'a [S::Id],
    ) -> Option<IndexQuery<'a, S::Id, S::Value>> {
        // check what columns this plan must select

        // check how much of the given range this plan supports without contradicting the given order

        // return a descriptor of which part of the range and order can be used to select which columns

        todo!()
    }

    fn supports<S: Schema>(
        &self,
        schema: &TableSchema<S>,
        range: &Range<S::Id, S::Value>,
        order: &[S::Id],
    ) -> bool {
        let mut supported_range = SmallVec::<[&S::Id; ROW_STACK_SIZE]>::with_capacity(range.len());
        let mut supported_order = SmallVec::<[&S::Id; ROW_STACK_SIZE]>::with_capacity(order.len());

        for (index_id, query) in &self.indices {
            let index = schema.get_index(*index_id);
            let index_columns = &index.columns()[..query.select];

            for col_name in index_columns {
                match range.get(col_name) {
                    None => break,
                    Some(col_range) => {
                        if !supported_range.contains(&col_name) {
                            supported_range.push(col_name);
                        }

                        if col_range.is_range() {
                            break;
                        }
                    }
                }
            }

            for (col_name, order) in index_columns.iter().zip(&order[supported_order.len()..]) {
                if col_name == order {
                    supported_order.push(col_name);
                } else {
                    break;
                }
            }
        }

        range.len() == supported_range.len() && order.len() == supported_order.len()
    }
}

#[derive(Eq, PartialEq)]
pub(crate) struct TableSchema<S> {
    inner: S,
}

impl<S> TableSchema<S> {
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: Schema> TableSchema<S> {
    #[inline]
    pub fn extract_key(&self, index_name: &str, row: &[S::Value]) -> b_tree::Key<S::Value> {
        todo!()
    }

    #[inline]
    fn get_index<'a>(&'a self, index_id: IndexId<'a>) -> &'a S::Index {
        match index_id {
            IndexId::Primary => self.primary(),
            IndexId::Auxiliary(index_id) => self
                .auxiliary()
                .iter()
                .filter_map(|(name, index)| if name == index_id { Some(index) } else { None })
                .next()
                .expect("index"),
        }
    }

    pub fn indices(&self) -> impl Iterator<Item = (IndexId, &S::Index)> {
        let primary = (IndexId::Primary, self.inner.primary());

        let aux = self
            .inner
            .auxiliary()
            .iter()
            .map(|(name, index)| (name.into(), index));

        iter::once(primary).chain(aux)
    }
}

impl<S> Deref for TableSchema<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Schema> From<S> for TableSchema<S> {
    fn from(inner: S) -> Self {
        Self { inner }
    }
}
