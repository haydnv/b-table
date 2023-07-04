use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::ops::Bound;
use std::{fmt, io};

use b_tree::collate::{Collate, Overlap, OverlapsRange, OverlapsValue};

pub use b_tree::{Key, Schema as BTreeSchema};

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum IndexId {
    Primary,
    Aux(usize),
}

#[derive(Eq, PartialEq)]
pub struct QueryPlan<'a, S> {
    schema: &'a S,
    indices: Vec<IndexId>,
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
    fn with_index(
        schema: &'a S,
        order: &[S::Id],
        range: &Range<S::Id, S::Value>,
        index_id: IndexId,
    ) -> Option<Self> {
        let index = get_index(schema, index_id);
        if index.columns().is_empty() {
            None
        } else if order.is_empty() {
            if range.is_default() {
                Some(Self {
                    schema,
                    indices: vec![index_id],
                })
            } else if range.columns.contains_key(&index.columns()[0]) {
                let mut indices = Vec::with_capacity((schema.auxiliary().len() + 1) * 2);
                indices.push(index_id);
                Some(Self { schema, indices })
            } else {
                None
            }
        } else if index.columns()[0] == order[0] {
            let mut indices = Vec::with_capacity((schema.auxiliary().len() + 1) * 2);
            indices.push(index_id);
            Some(Self { schema, indices })
        } else {
            None
        }
    }

    fn clone_and_push(&self, index_id: IndexId) -> Self {
        let mut clone = self.clone();
        clone.indices.push(index_id);
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
                            if range.columns.contains_key(col_name) {
                                covered_range.insert(col_name);
                            }

                            index_covers += 1;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            } else {
                for col_name in index.columns() {
                    if range.columns.contains_key(col_name) {
                        covered_range.insert(col_name);
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

    fn needs(&self, order: &[S::Id], range: &Range<S::Id, S::Value>, index_id: IndexId) -> bool {
        let (covered_order, covered_range) = self.covers(order, range);
        debug_assert!(covered_order <= order.len());

        let index_columns = get_index(self.schema, index_id).columns();
        if index_columns.is_empty() {
            false
        } else if covered_order < order.len() {
            index_columns.starts_with(&order[covered_order..covered_order + 1])
        } else {
            range.columns.contains_key(&index_columns[0])
                && !covered_range.contains(&index_columns[0])
        }
    }
}

/// The schema of a table index
pub trait IndexSchema: BTreeSchema + Clone {
    type Id: Hash + Eq + fmt::Debug + fmt::Display;

    /// Borrow the list of columns specified by this schema.
    fn columns(&self) -> &[Self::Id];

    // TODO: delete
    /// Given a key matching this [`Schema`], extract a key matching the `other` [`Schema`].
    /// This values in `key` must be in order, but the values in `other` may be in any order.
    /// Panics: if `other` is not a subset of `self`.
    fn extract_key(&self, key: &[Self::Value], other: &Self) -> Key<Self::Value>;

    // TODO: delete
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

    // TODO: delete
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
    type Id: Hash + Eq + fmt::Debug + fmt::Display;
    type Error: std::error::Error + From<io::Error>;
    type Value: Clone + Eq + fmt::Debug + 'static;
    type Index: IndexSchema<Error = Self::Error, Id = Self::Id, Value = Self::Value> + 'static;

    /// Borrow the names of the columns in the primary key.
    fn key(&self) -> &[Self::Id];

    /// Borrow the names of the value columns.
    fn values(&self) -> &[Self::Id];

    /// Borrow the schema of the primary index.
    fn primary(&self) -> &Self::Index;

    /// Borrow the schemata of the auxiliary indices.
    /// This is ordered so that the first index which matches a given [`Range`] will be used.
    fn auxiliary(&self) -> &[(String, Self::Index)];

    // TODO: delete
    fn extract_key(
        &self,
        index_key: Vec<Self::Value>,
        index_schema: &Self::Index,
    ) -> Vec<Self::Value> {
        assert_eq!(index_key.len(), BTreeSchema::len(index_schema));
        assert!(self
            .key()
            .iter()
            .all(|col_name| index_schema.columns().contains(col_name)));

        let mut key = Vec::with_capacity(self.key().len());
        for key_col_name in self.key() {
            for (index_col_name, value) in index_schema.columns().iter().zip(&index_key) {
                if key_col_name == index_col_name {
                    key.push(value.clone());
                }
            }
        }

        key
    }

    /// Compute a sequence of indices to read from in order construct a result set
    /// with the given range and order.
    fn plan_query<'a>(
        &'a self,
        order: &[Self::Id],
        range: &Range<Self::Id, Self::Value>,
    ) -> Result<QueryPlan<'a, Self>, io::Error> {
        let mut candidates = VecDeque::with_capacity(self.auxiliary().len() * 2);

        if let Some(candidate) = QueryPlan::with_index(self, order, range, IndexId::Primary) {
            if candidate.is_complete(order, range) {
                return Ok(candidate);
            } else {
                candidates.push_back(candidate);
            }
        }

        for index_id in (0..self.auxiliary().len()).into_iter().map(IndexId::Aux) {
            if let Some(candidate) = QueryPlan::with_index(self, order, range, index_id) {
                if candidate.is_complete(order, range) {
                    return Ok(candidate);
                } else {
                    candidates.push_back(candidate);
                }
            }
        }

        while let Some(plan) = candidates.pop_front() {
            if plan.needs(order, range, IndexId::Primary) {
                let candidate = plan.clone_and_push(IndexId::Primary);
                if candidate.is_complete(order, range) {
                    return Ok(candidate);
                } else {
                    candidates.push_back(candidate);
                }
            }

            for index_id in (0..self.auxiliary().len()).into_iter().map(IndexId::Aux) {
                if plan.needs(order, range, index_id) {
                    let candidate = plan.clone_and_push(index_id);
                    if candidate.is_complete(order, range) {
                        return Ok(candidate);
                    } else {
                        candidates.push_back(candidate);
                    }
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

fn get_index<S: Schema>(schema: &S, index_id: IndexId) -> &S::Index {
    match index_id {
        IndexId::Primary => schema.primary(),
        IndexId::Aux(i) => &schema.auxiliary()[i].1,
    }
}
