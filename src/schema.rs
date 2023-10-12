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
pub(crate) struct IndexQuery<'a, K> {
    pub range: Columns<'a, K>,
    pub select: usize,
}

impl<'a, K: fmt::Debug> fmt::Debug for IndexQuery<'a, K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "query {} columns with a range over {:?}",
            self.select, self.range
        )
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

        if candidate.supports(schema, range, order) {
            return Some(candidate);
        }

        let mut candidates = VecDeque::with_capacity((schema.auxiliary().len() + 1) * 2);
        candidates.push_back(candidate);

        while let Some(candidate) = candidates.pop_front() {
            for index_id in schema.index_ids() {
                // check whether this candidate needs the given index
                // if so, create a new candidate with this index name and add it to the queue
                if let Some(query) = candidate.needs::<S>(schema, index_id, range, order) {
                    let candidate = candidate.clone_and_push(index_id, query);

                    if candidate.supports(schema, range, order) {
                        return Some(candidate);
                    } else {
                        println!("{candidate:?} does not support {range:?} with order {order:?}");
                        candidates.push_back(candidate);
                    }
                }
            }
        }

        None
    }

    fn clone_and_push(&self, index_id: IndexId<'a>, query: IndexQuery<'a, K>) -> Self {
        let mut clone = self.clone();
        clone.indices.push((index_id, query));
        clone
    }

    fn needs<S>(
        &self,
        schema: &'a TableSchema<S>,
        index_id: IndexId<'a>,
        range: &HashMap<K, ColumnRange<S::Value>>,
        order: &'a [K],
    ) -> Option<IndexQuery<'a, K>>
    where
        S: Schema<Id = K>,
    {
        // check what columns are already selected by this plan
        // and which range columns are already covered
        let mut selected = Columns::with_capacity(schema.primary().len());
        let mut covered = Columns::with_capacity(range.len());

        for (index_id, query) in &self.indices {
            let index = schema.get_index(*index_id).expect("index");

            for col_name in &index.columns()[..query.select] {
                if !selected.contains(&col_name) {
                    selected.push(col_name);
                }
            }

            for col_name in &query.range {
                assert!(!covered.contains(col_name));
                covered.push(*col_name);
            }
        }

        // assert that the covered columns don't contradict the range
        assert!(covered.len() <= range.len());
        assert!(covered.iter().all(|c| range.contains_key(c)));

        // assert that the selected columns don't contradict the order
        assert!(order
            .iter()
            .take(selected.len())
            .all(|c| selected.contains(&c)));

        println!("selected columns: {selected:?}");

        // now that this plan has been characterized, check if it needs this index
        let index = schema.get_index(index_id).expect("index");

        let already_selected = index
            .columns()
            .iter()
            .take_while(|c| selected.contains(c))
            .count();

        // first, check if any range columns remain un-covered
        let uncovered_range = if covered.len() < range.len() {
            // and if so which of them this index can cover without contradicting the order
            let mut uncovered = Columns::with_capacity(range.len() - covered.len());

            println!(
                "{} range columns are still not covered",
                range.len() - covered.len()
            );

            for col_name in &index.columns()[already_selected..] {
                if let Some(col_range) = range.get(col_name) {
                    uncovered.push(col_name);

                    if col_range.is_range() {
                        break;
                    }
                } else {
                    break;
                }
            }

            println!("uncovered range columns: {uncovered:?}");

            uncovered
        } else {
            Default::default()
        };

        assert!(covered.len() + uncovered_range.len() <= range.len());

        let select = if covered.len() + uncovered_range.len() == range.len() {
            // then, if the entire range is covered, check if this index can provide the remaining order
            let mut select = uncovered_range.len();

            let query_order = order.iter().skip(selected.len() + uncovered_range.len());
            let index_order = &index.columns()[already_selected + uncovered_range.len()..];

            println!("query order is {query_order:?}, index order is {index_order:?}, already selected {already_selected}");

            select += index_order
                .into_iter()
                .zip(query_order)
                .take_while(|(i, q)| i == q)
                .count();

            println!(
                "order columns: {:?}",
                &index.columns()[..already_selected + select]
            );

            select
        } else {
            uncovered_range.len()
        };

        // if this index provides range or order coverage, it is needed
        if select > 0 {
            Some(IndexQuery {
                range: uncovered_range,
                select: already_selected + select,
            })
        } else {
            None
        }
    }

    fn supports<S>(
        &self,
        schema: &TableSchema<S>,
        range: &HashMap<K, ColumnRange<S::Value>>,
        order: &[K],
    ) -> bool
    where
        S: Schema<Id = K>,
    {
        let mut supported_range = Columns::with_capacity(range.len());
        let mut supported_order = Columns::with_capacity(order.len());

        for (index_id, query) in &self.indices {
            let index = schema.get_index(*index_id).expect("index");

            for col_name in &query.range {
                assert!(!supported_range.contains(&col_name));
                supported_range.push(col_name);

                if supported_order.len() < order.len() {
                    supported_order.push(*col_name);
                }
            }

            for i in query.range.len()..query.select {
                let col_name = &index.columns()[i];

                if supported_order.len() < order.len() && !supported_order.contains(&col_name) {
                    supported_order.push(col_name);
                }
            }
        }

        println!(
            "{self:?} supports range columns {supported_range:?} and order {supported_order:?}"
        );

        range.len() == supported_range.len() && order.len() == supported_order.len()
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

        let range = HashMap::default();

        let schema = schema.into();
        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");

        let expected = QueryPlan::default();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_query_plan_primary_index_one_column_range() {
        let schema = TestTable {
            primary: TestIndex {
                columns: vec!["0", "1", "2", "3", "value"],
            },
            aux: vec![],
        };

        let mut range = HashMap::new();
        range.insert("0", (1..2).into());

        let schema = schema.into();
        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");

        let expected = QueryPlan {
            indices: smallvec![(
                IndexId::Primary,
                IndexQuery {
                    range: smallvec![&"0"],
                    select: 1,
                }
            ),],
        };

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

        let mut expected = QueryPlan {
            indices: [
                (
                    IndexId::Primary,
                    IndexQuery {
                        range: smallvec![&"0"],
                        select: 1,
                    },
                ),
                (
                    IndexId::Primary,
                    IndexQuery {
                        range: smallvec![&"1"],
                        select: 2,
                    },
                ),
            ]
            .into_iter()
            .collect(),
        };

        {
            let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");
            assert_eq!(expected, actual);
        }

        {
            let actual = QueryPlan::new(&schema, &range, &["0"]).expect("plan");
            assert_eq!(expected, actual);
        }

        {
            let actual = QueryPlan::new(&schema, &range, &["0", "1"]).expect("plan");
            assert_eq!(expected, actual);
        }

        {
            expected.indices[1].1.select = 3;
            let actual = QueryPlan::new(&schema, &range, &["0", "1", "2"]).expect("plan");
            assert_eq!(expected, actual);
        }

        {
            expected.indices[1].1.select = 4;
            let actual = QueryPlan::new(&schema, &range, &["0", "1", "2", "3"]).expect("plan");
            assert_eq!(expected, actual);
        }
    }
}
