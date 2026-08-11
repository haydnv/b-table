use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Bound, Deref};
use std::{fmt, io, iter};

use b_tree::collate::*;

use crate::plan::QueryPlan;
pub use b_tree::Schema as BTreeSchema;

/// An ID type used to look up a specific table index
#[derive(Copy, Clone, Eq, PartialEq, Hash, Default)]
pub enum IndexId<'a> {
    #[default]
    Primary,
    Auxiliary(&'a str),
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

/// The schema of a table index
pub trait IndexSchema: BTreeSchema + Clone + Send + Sync + 'static {
    type Id: Hash + Eq + Clone + fmt::Debug + fmt::Display + 'static;

    /// Borrow the list of columns specified by this schema.
    fn columns(&self) -> &[Self::Id];

    /// Return `true` if an index with this schema supports the given [`Range`].
    fn supports(&self, range: &Range<Self::Id, Self::Value>) -> bool {
        range_is_supported(&range.columns, self.columns())
    }
}

/// The schema of a [`Table`]
pub trait Schema: Eq + Send + Sync + Sized + fmt::Debug {
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
    /// Return `true` if this [`ColumnRange`] covers more than a single value.
    pub fn is_range(&self) -> bool {
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

impl<V> From<std::ops::Range<V>> for ColumnRange<V> {
    fn from(range: std::ops::Range<V>) -> Self {
        Self::In((Bound::Included(range.start), Bound::Excluded(range.end)))
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
    /// Borrow this [`Range`]'s underlying [`HashMap`] of [`ColumnRange`]s.
    pub fn inner(&self) -> &HashMap<K, ColumnRange<V>> {
        &self.columns
    }

    /// Destructure this [`Range`] into a [`HashMap`] of [`ColumnRange`]s.
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K: Eq + Hash, V> Range<K, V> {
    /// Get a [`ColumnRange`] in this range, if specified.
    pub fn get(&self, column: &K) -> Option<&ColumnRange<V>> {
        self.columns.get(column)
    }
}

impl<K: Clone + Eq + Hash, V: Clone> Range<K, V> {
    /// Intersect this range with `other`, or return `None` if they are disjoint.
    pub fn intersection<C>(&self, other: Self, collator: &C) -> Option<Self>
    where
        C: Collate<Value = V>,
    {
        let mut columns = self.columns.clone();
        for (name, other) in other.columns {
            let intersection = match columns.remove(&name) {
                Some(this) => intersect_column(this, other, collator)?,
                None => other,
            };
            columns.insert(name, intersection);
        }
        Some(Self { columns })
    }
}

fn intersect_column<C>(
    left: ColumnRange<C::Value>,
    right: ColumnRange<C::Value>,
    collator: &C,
) -> Option<ColumnRange<C::Value>>
where
    C: Collate,
    C::Value: Clone,
{
    match (left, right) {
        (ColumnRange::Eq(left), ColumnRange::Eq(right)) => {
            (collator.cmp(&left, &right) == Ordering::Equal).then_some(ColumnRange::Eq(left))
        }
        (ColumnRange::Eq(value), ColumnRange::In(bounds))
        | (ColumnRange::In(bounds), ColumnRange::Eq(value)) => {
            contains(&bounds, &value, collator).then_some(ColumnRange::Eq(value))
        }
        (ColumnRange::In(left), ColumnRange::In(right)) => {
            let lower = max_lower(left.0, right.0, collator);
            let upper = min_upper(left.1, right.1, collator);
            valid_bounds(&lower, &upper, collator).then_some(ColumnRange::In((lower, upper)))
        }
    }
}

fn contains<C>(bounds: &(Bound<C::Value>, Bound<C::Value>), value: &C::Value, collator: &C) -> bool
where
    C: Collate,
{
    let above_lower = match &bounds.0 {
        Bound::Unbounded => true,
        Bound::Included(lower) => collator.cmp(value, lower) != Ordering::Less,
        Bound::Excluded(lower) => collator.cmp(value, lower) == Ordering::Greater,
    };
    let below_upper = match &bounds.1 {
        Bound::Unbounded => true,
        Bound::Included(upper) => collator.cmp(value, upper) != Ordering::Greater,
        Bound::Excluded(upper) => collator.cmp(value, upper) == Ordering::Less,
    };
    above_lower && below_upper
}

fn max_lower<C>(left: Bound<C::Value>, right: Bound<C::Value>, collator: &C) -> Bound<C::Value>
where
    C: Collate,
{
    match (&left, &right) {
        (Bound::Unbounded, _) => right,
        (_, Bound::Unbounded) => left,
        (
            Bound::Included(left_value) | Bound::Excluded(left_value),
            Bound::Included(right_value) | Bound::Excluded(right_value),
        ) => match collator.cmp(left_value, right_value) {
            Ordering::Less => right,
            Ordering::Greater => left,
            Ordering::Equal if matches!(left, Bound::Excluded(_)) => left,
            Ordering::Equal => right,
        },
    }
}

fn min_upper<C>(left: Bound<C::Value>, right: Bound<C::Value>, collator: &C) -> Bound<C::Value>
where
    C: Collate,
{
    match (&left, &right) {
        (Bound::Unbounded, _) => right,
        (_, Bound::Unbounded) => left,
        (
            Bound::Included(left_value) | Bound::Excluded(left_value),
            Bound::Included(right_value) | Bound::Excluded(right_value),
        ) => match collator.cmp(left_value, right_value) {
            Ordering::Less => left,
            Ordering::Greater => right,
            Ordering::Equal if matches!(left, Bound::Excluded(_)) => left,
            Ordering::Equal => right,
        },
    }
}

fn valid_bounds<C>(lower: &Bound<C::Value>, upper: &Bound<C::Value>, collator: &C) -> bool
where
    C: Collate,
{
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Included(lower), Bound::Included(upper)) => {
            collator.cmp(lower, upper) != Ordering::Greater
        }
        (
            Bound::Included(lower) | Bound::Excluded(lower),
            Bound::Included(upper) | Bound::Excluded(upper),
        ) => collator.cmp(lower, upper) == Ordering::Less,
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
    pub fn get_index<'a>(&'a self, index_id: IndexId<'a>) -> Option<&'a S::Index> {
        match index_id {
            IndexId::Primary => Some(self.primary()),
            IndexId::Auxiliary(index_id) => self
                .auxiliary()
                .iter()
                .filter_map(|(name, index)| if name == index_id { Some(index) } else { None })
                .next(),
        }
    }

    pub fn index_ids(&self) -> impl Iterator<Item = IndexId<'_>> {
        let aux = self.inner.auxiliary().iter().map(|(name, _)| name.into());

        iter::once(IndexId::Primary).chain(aux)
    }

    pub fn plan_query<'a>(
        &'a self,
        range: &HashMap<S::Id, ColumnRange<S::Value>>,
        order: &'a [S::Id],
        select: &'a [S::Id],
    ) -> Result<QueryPlan<'a, S::Id>, io::Error> {
        QueryPlan::new(self, range, order, select).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "{:?} has no index to support range {range:?} and order {order:?}",
                    self.inner
                ),
            )
        })
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

#[inline]
pub(crate) fn range_is_supported<K, V>(range: &HashMap<K, ColumnRange<V>>, columns: &[K]) -> bool
where
    K: Eq + PartialEq + Hash,
{
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

#[cfg(test)]
mod tests {
    use super::*;

    fn range(lower: Bound<u64>, upper: Bound<u64>) -> Range<&'static str, u64> {
        [("id", ColumnRange::In((lower, upper)))]
            .into_iter()
            .collect()
    }

    #[test]
    fn range_intersection_preserves_the_narrowest_bounds() {
        let outer = range(Bound::Included(1), Bound::Excluded(4));
        let inner = range(Bound::Included(2), Bound::Excluded(3));
        let intersection = outer
            .intersection(inner, &Collator::default())
            .expect("overlapping ranges");
        assert_eq!(
            intersection.get(&"id"),
            Some(&ColumnRange::In((Bound::Included(2), Bound::Excluded(3))))
        );
    }

    #[test]
    fn range_intersection_rejects_disjoint_bounds() {
        let outer = range(Bound::Included(1), Bound::Excluded(2));
        let inner = range(Bound::Included(2), Bound::Excluded(3));
        assert!(outer.intersection(inner, &Collator::default()).is_none());
    }
}
