use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::hash::Hash;
use std::ops::{Bound, Deref};
use std::{fmt, io, iter};

use b_tree::collate::*;
use smallvec::SmallVec;

use crate::table::ROW_STACK_SIZE;
use crate::IndexStack;

pub use b_tree::Schema as BTreeSchema;

type Columns<'a, K> = SmallVec<[&'a K; ROW_STACK_SIZE]>;

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

/// The schema of a table index
pub trait IndexSchema: BTreeSchema + Clone {
    type Id: Hash + Eq + Clone + fmt::Debug + fmt::Display;

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

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum IndexQuery<'a, K> {
    Range(Columns<'a, K>),
    Order(&'a [K]),
    RangeAndOrder(Columns<'a, K>, &'a [K]),
}

impl<'a, K> IndexQuery<'a, K> {
    #[inline]
    pub fn range(&self) -> Columns<'a, K> {
        match self {
            Self::Range(range) => Columns::from_slice(range),
            Self::Order(_order) => Columns::default(),
            Self::RangeAndOrder(range, _order) => Columns::from_slice(range),
        }
    }

    #[inline]
    pub fn selected(&self, prefix_len: usize) -> usize {
        match self {
            Self::Range(range) => prefix_len + range.len(),
            Self::Order(order) => {
                debug_assert!(prefix_len < order.len());
                order.len()
            }
            Self::RangeAndOrder(range, order) => {
                debug_assert!(range.len() < order.len());
                debug_assert!(prefix_len < order.len());
                order.len()
            }
        }
    }
}

impl<'a, K: fmt::Debug> fmt::Debug for IndexQuery<'a, K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Range(columns) => {
                write!(
                    f,
                    "query {} column(s) with a range over {:?}",
                    columns.len(),
                    columns
                )
            }
            Self::Order(order) => {
                write!(f, "query {} column(s)", order.len())
            }
            Self::RangeAndOrder(range, order) => {
                write!(
                    f,
                    "query {} column(s) with a range over {:?}",
                    order.len(),
                    range
                )
            }
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct QueryPlan<'a, K> {
    pub indices: IndexStack<(IndexId<'a>, IndexQuery<'a, K>)>,
}

impl<'a, K> Default for QueryPlan<'a, K> {
    fn default() -> Self {
        Self {
            indices: IndexStack::default(),
        }
    }
}

impl<'a, K: Eq> Ord for QueryPlan<'a, K> {
    fn cmp(&self, other: &Self) -> Ordering {
        // inverse order based on number of indices to query
        other.indices.len().cmp(&self.indices.len())
    }
}

impl<'a, K: Eq> PartialOrd for QueryPlan<'a, K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, K: Clone + Eq + Hash + fmt::Debug> QueryPlan<'a, K> {
    pub fn new<S>(
        schema: &'a TableSchema<S>,
        range: &HashMap<K, ColumnRange<S::Value>>,
        order: &'a [K],
    ) -> Option<Self>
    where
        S: Schema<Id = K>,
    {
        let candidate = Self::default();

        if range_is_supported(range, schema.primary().columns())
            && schema.primary().columns().starts_with(order)
        {
            return Some(candidate);
        }

        let mut candidates = BinaryHeap::with_capacity((schema.auxiliary().len() + 1) * 2);

        let mut unvisited = VecDeque::with_capacity(candidates.capacity());
        unvisited.push_back(candidate);

        // first, create a list of all candidates which cover the requested order (if any)
        while let Some(candidate) = unvisited.pop_front() {
            if candidates.iter().any(|c| c == &candidate) {
                continue;
            }

            let supported_order = candidate.supported_order(schema, order);
            let supported_range = candidate.supported_range(schema, range);
            let selected = candidate.selected(schema);

            debug_assert!(order.starts_with(supported_order));

            for index_id in schema.index_ids() {
                let index = schema.get_index(index_id).expect("index");

                if !index.columns().starts_with(selected) {
                    continue;
                }

                let mut covered_range = Columns::with_capacity(index.len());
                for (i, col_name) in index.columns()[selected.len()..].iter().enumerate() {
                    if let Some(order_col) = order.get(supported_order.len() + i) {
                        if col_name != order_col {
                            break;
                        }
                    }

                    if supported_range.contains(&col_name) {
                        break;
                    } else if let Some(col_range) = range.get(col_name) {
                        covered_range.push(col_name);

                        if col_range.is_range() {
                            break;
                        }
                    }
                }

                if supported_order.len() < order.len() {
                    let covered_order = index
                        .columns()
                        .iter()
                        .skip(selected.len())
                        .zip(&order[supported_order.len()..])
                        .take_while(|(ic, oc)| ic == oc)
                        .count();

                    let index_order = &index.columns()[..selected.len() + covered_order];

                    if covered_order > 0 {
                        let query = IndexQuery::Order(index_order);
                        unvisited.push_back(candidate.clone_and_push(index_id, query));

                        if !covered_range.is_empty() && covered_range.len() < covered_order {
                            let covered_range = Columns::from_slice(&covered_range);
                            let query = IndexQuery::RangeAndOrder(covered_range, index_order);
                            let candidate = candidate.clone_and_push(index_id, query);
                            unvisited.push_back(candidate);
                        }
                    }
                }

                if !covered_range.is_empty() {
                    let query = IndexQuery::Range(covered_range);
                    let candidate = candidate.clone_and_push(index_id, query);
                    unvisited.push_back(candidate);
                }
            }

            if supported_order == order {
                candidates.push(candidate);
            }
        }

        // then winnow down to only candidates which support the largest portion of the range
        let max_supported_range = candidates
            .iter()
            .map(|candidate| candidate.supported_range(schema, range).len())
            .fold(0, Ord::max);

        // then winnow by candidate size (number of indices to query)
        while let Some(candidate) = candidates.pop() {
            if candidate.supported_range(schema, range).len() == max_supported_range {
                return Some(candidate);
            }
        }

        None
    }

    fn clone_and_push(&self, index_id: IndexId<'a>, query: IndexQuery<'a, K>) -> Self {
        let mut clone = self.clone();
        clone.indices.push((index_id, query));
        clone
    }

    fn selected<'b, S>(&self, schema: &'b TableSchema<S>) -> &'b [K]
    where
        S: Schema<Id = K>,
        'a: 'b,
    {
        let mut selected = &schema.primary().columns()[..0];

        for (index_id, query) in &self.indices {
            let index = schema.get_index(*index_id).expect("index");
            selected = &index.columns()[..query.selected(selected.len())];
        }

        selected
    }

    fn supported_order<S>(&self, schema: &TableSchema<S>, order: &'a [K]) -> &'a [K]
    where
        S: Schema<Id = K>,
    {
        let mut i = 0;
        let mut selected = &schema.primary().columns()[..0];

        for (index_id, query) in &self.indices {
            let index = schema.get_index(*index_id).expect("index");

            let offset = selected.len();

            i += order
                .iter()
                .skip(i)
                .zip(&index.columns()[offset..query.selected(offset)])
                .take_while(|(o, c)| o == c)
                .count();

            selected = &index.columns()[..query.selected(offset)];
        }

        &order[..i]
    }

    fn supported_range<'b, S>(
        &self,
        schema: &'b TableSchema<S>,
        range: &HashMap<K, ColumnRange<S::Value>>,
    ) -> Columns<'b, K>
    where
        S: Schema<Id = K>,
        'a: 'b,
    {
        let mut columns = Columns::with_capacity(schema.primary().len());
        let mut selected = &schema.primary().columns()[..0];

        for (index_id, query) in &self.indices {
            let index = schema.get_index(*index_id).expect("index");

            for col_name in query.range() {
                debug_assert!(
                    !columns.contains(&col_name),
                    "range {col_name:?} is already covered by {columns:?}"
                );

                columns.push(col_name);
            }

            selected = &index.columns()[..query.selected(selected.len())];
        }

        columns
    }
}

impl<'a, K: fmt::Debug> fmt::Debug for QueryPlan<'a, K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "plan to query {:?}", self.indices)
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
    pub fn extract_key(&self, index_id: IndexId, row: &[S::Value]) -> b_tree::Key<S::Value> {
        let index = self.get_index(index_id).expect("index");

        index
            .columns()
            .iter()
            .filter_map(|col_name| self.primary().columns().iter().position(|c| c == col_name))
            .map(|i| row[i].clone())
            .collect()
    }

    #[inline]
    fn get_index<'a>(&'a self, index_id: IndexId<'a>) -> Option<&'a S::Index> {
        match index_id {
            IndexId::Primary => Some(self.primary()),
            IndexId::Auxiliary(index_id) => self
                .auxiliary()
                .iter()
                .filter_map(|(name, index)| if name == index_id { Some(index) } else { None })
                .next(),
        }
    }

    pub fn index_ids(&self) -> impl Iterator<Item = IndexId> {
        let aux = self.inner.auxiliary().iter().map(|(name, _)| name.into());

        iter::once(IndexId::Primary).chain(aux)
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

#[inline]
fn range_is_supported<K, V>(range: &HashMap<K, ColumnRange<V>>, columns: &[K]) -> bool
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
mod test {
    use std::{fmt, io};

    use smallvec::smallvec;

    use super::*;

    #[derive(Clone, Eq, PartialEq)]
    struct TestIndex {
        columns: Vec<&'static str>,
    }

    impl b_tree::Schema for TestIndex {
        type Error = io::Error;
        type Value = u64;

        fn block_size(&self) -> usize {
            4_096
        }

        fn len(&self) -> usize {
            self.columns.len()
        }

        fn order(&self) -> usize {
            10
        }

        fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
            if key.len() == self.len() {
                Ok(key)
            } else {
                unimplemented!()
            }
        }
    }

    impl IndexSchema for TestIndex {
        type Id = &'static str;

        fn columns(&self) -> &[Self::Id] {
            &self.columns
        }
    }

    impl fmt::Debug for TestIndex {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "index schema with columns {:?}", self.columns)
        }
    }

    #[derive(Clone, Eq, PartialEq)]
    struct TestTable {
        primary: TestIndex,
        aux: Vec<(String, TestIndex)>,
    }

    impl Schema for TestTable {
        type Id = &'static str;
        type Error = io::Error;
        type Value = u64;
        type Index = TestIndex;

        fn key(&self) -> &[Self::Id] {
            &self.primary.columns()[..self.primary().len() - 1]
        }

        fn values(&self) -> &[Self::Id] {
            &self.primary.columns[self.primary.len()..]
        }

        fn primary(&self) -> &Self::Index {
            &self.primary
        }

        fn auxiliary(&self) -> &[(String, Self::Index)] {
            &self.aux
        }

        fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
            if key.len() == self.key().len() {
                Ok(key)
            } else {
                unimplemented!()
            }
        }

        fn validate_values(
            &self,
            values: Vec<Self::Value>,
        ) -> Result<Vec<Self::Value>, Self::Error> {
            if values.len() == self.values().len() {
                Ok(values)
            } else {
                unimplemented!()
            }
        }
    }

    impl fmt::Debug for TestTable {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("table schema")
        }
    }

    #[test]
    fn test_query_plan_default() {
        let schema = TestTable {
            primary: TestIndex {
                columns: vec!["0", "1", "2", "3", "value"],
            },
            aux: vec![],
        };

        let schema = schema.into();

        let expected = QueryPlan::default();

        let range = HashMap::default();
        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");
        assert_eq!(expected, actual);

        let mut range = HashMap::new();
        range.insert("0", 0.into());
        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");
        assert_eq!(expected, actual);

        let mut range = HashMap::new();
        range.insert("0", 1.into());
        range.insert("1", (1..2).into());
        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_query_plan_primary_index_two_column_range() {
        let schema = TestTable {
            primary: TestIndex {
                columns: vec!["0", "1", "2", "3", "value"],
            },
            aux: vec![],
        };

        let schema = schema.into();

        let mut range = HashMap::new();
        range.insert("0", (1..2).into());
        range.insert("1", (2..3).into());

        let expected = QueryPlan {
            indices: [
                (IndexId::Primary, IndexQuery::Range(smallvec![&"0"])),
                (IndexId::Primary, IndexQuery::Range(smallvec![&"1"])),
            ]
            .into_iter()
            .collect(),
        };

        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");
        assert_eq!(expected, actual);

        let actual = QueryPlan::new(&schema, &range, &["0"]).expect("plan");
        assert_eq!(expected, actual);

        let actual = QueryPlan::new(&schema, &range, &["0", "1"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: [
                (IndexId::Primary, IndexQuery::Range(smallvec![&"0"])),
                (
                    IndexId::Primary,
                    IndexQuery::RangeAndOrder(smallvec![&"1"], &["0", "1", "2"]),
                ),
            ]
            .into_iter()
            .collect(),
        };

        let actual = QueryPlan::new(&schema, &range, &["0", "1", "2"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: [
                (IndexId::Primary, IndexQuery::Range(smallvec![&"0"])),
                (
                    IndexId::Primary,
                    IndexQuery::RangeAndOrder(smallvec![&"1"], &["0", "1", "2", "3"]),
                ),
            ]
            .into_iter()
            .collect(),
        };

        let actual = QueryPlan::new(&schema, &range, &["0", "1", "2", "3"]).expect("plan");
        assert_eq!(expected, actual);
    }
}
