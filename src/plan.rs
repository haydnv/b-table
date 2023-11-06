use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt;
use std::hash::Hash;

use smallvec::SmallVec;

use super::schema::*;
use super::table::ROW_STACK_SIZE;
use super::{IndexStack, INDEX_STACK_SIZE};

type Columns<'a, K> = SmallVec<[&'a K; ROW_STACK_SIZE]>;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct IndexQuery<'a, K> {
    columns: &'a [K],
    range: Columns<'a, K>,
    order: usize,
}

impl<'a, K: Eq> IndexQuery<'a, K> {
    #[inline]
    pub fn new(columns: &'a [K], range: Columns<'a, K>, order: usize) -> Self {
        debug_assert!(order <= columns.len());
        debug_assert!(range.iter().all(|c| columns.contains(c)));

        Self {
            columns,
            range,
            order,
        }
    }

    #[inline]
    pub fn order(&self) -> &'a [K] {
        &self.columns[..self.order]
    }

    #[inline]
    pub fn range(&self) -> &Columns<'a, K> {
        &self.range
    }

    #[inline]
    pub fn selected(&self, prefix_len: usize) -> usize {
        prefix_len + self.columns.len()
    }

    #[inline]
    pub fn selects(&self, col_name: &'a K) -> bool {
        self.columns.contains(col_name)
    }
}

impl<'a, K: fmt::Debug> fmt::Debug for IndexQuery<'a, K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "query {:?} column(s) to order by the first {} with a range over {:?}",
            self.columns.len(),
            self.order,
            self.range
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
            let supported_order = candidate.supported_order(order);
            let supported_range = candidate.supported_range();
            let selected = candidate.selected(schema);

            debug_assert!(order.starts_with(supported_order));

            for index_id in schema.index_ids() {
                let candidates = candidate.needs(
                    schema,
                    index_id,
                    selected,
                    &supported_range,
                    supported_order,
                    range,
                    order,
                );

                unvisited.extend(candidates);
            }

            debug_assert!(supported_range.iter().all(|c| range.contains_key(c)));

            if supported_order == order
                && supported_range.len() == range.len()
                && schema.key().iter().all(|c| candidate.selects(c))
            {
                candidates.push(candidate);
            }
        }

        candidates.pop()
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
        selected: &'a [K],
        supported_range: &Columns<'a, K>,
        supported_order: &'a [K],
        range: &HashMap<K, ColumnRange<S::Value>>,
        order: &'a [K],
    ) -> IndexStack<Self>
    where
        S: Schema<Id = K>,
    {
        debug_assert_eq!(*supported_range, self.supported_range());
        debug_assert_eq!(supported_order, self.supported_order(order));

        let index = schema.get_index(index_id).expect("index");

        debug_assert!(schema.key().iter().all(|c| index.columns().contains(c)));

        let present = selected
            .iter()
            .filter(|c| index.columns().contains(c))
            .count();

        debug_assert!(present <= index.columns().len());

        let mut unvisited = IndexStack::with_capacity(Ord::min(INDEX_STACK_SIZE, index.len() + 2));

        if selected.is_empty() {
            // pass
        } else if present == 0 {
            return unvisited;
        } else if index.columns()[present..].iter().any(|c| self.selects(c)) {
            return unvisited;
        } else if index.columns()[..present]
            .iter()
            .any(|c| !selected.contains(c))
        {
            return unvisited;
        }

        let mut covered_range = Columns::with_capacity(index.len());
        for (i, col_name) in index.columns()[present..].iter().enumerate() {
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
            } else {
                break;
            }
        }

        for i in (present..index.len()).into_iter().map(|i| i + 1) {
            let index_order = &index.columns()[present..i];

            let covered_order = index_order
                .iter()
                .zip(&order[supported_order.len()..])
                .take_while(|(ic, oc)| ic == oc)
                .count();

            let covered_range = covered_range
                .iter()
                .filter(|c| index_order.contains(c))
                .copied()
                .collect::<Columns<_>>();

            if covered_order > 0 || !covered_range.is_empty() {
                let query = IndexQuery::new(index_order, covered_range, covered_order);
                unvisited.push(self.clone_and_push(index_id, query));
            }
        }

        unvisited
    }

    fn selected<'b, S>(&self, schema: &'b TableSchema<S>) -> &'b [K]
    where
        S: Schema<Id = K>,
        'a: 'b,
    {
        let mut selected = &schema.primary().columns()[..0];

        for (index_id, query) in &self.indices {
            let index = schema.get_index(*index_id).expect("index");

            let present = selected
                .iter()
                .filter(|c| index.columns().contains(c))
                .count();

            if !selected.is_empty() {
                debug_assert!(present > 0);
            }

            selected = &index.columns()[..query.selected(present)];
        }

        selected
    }

    fn selects(&self, col_name: &'a K) -> bool {
        for (_index_id, query) in &self.indices {
            if query.selects(col_name) {
                return true;
            }
        }

        false
    }

    fn supported_order(&self, order: &'a [K]) -> &'a [K] {
        let mut i = 0;

        for (_index_id, query) in &self.indices {
            let index_order = query.order();

            debug_assert_eq!(
                &order[i..i + index_order.len()],
                index_order,
                "index order {index_order:?} does not match order {order:?} with prefix {:?}",
                &order[..i]
            );

            i += index_order.len();
        }

        &order[..i]
    }

    fn supported_range<'b>(&self) -> Columns<'b, K>
    where
        'a: 'b,
    {
        let mut columns = Columns::with_capacity(ROW_STACK_SIZE);

        for (_index_id, query) in &self.indices {
            for col_name in query.range() {
                debug_assert!(
                    !columns.contains(col_name),
                    "range {col_name:?} is already covered by {columns:?}"
                );

                columns.push(col_name);
            }
        }

        columns
    }
}

impl<'a, K: fmt::Debug> fmt::Debug for QueryPlan<'a, K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "plan to query {:?}", self.indices)
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
            indices: smallvec![
                (
                    IndexId::Primary,
                    IndexQuery::new(&["0"], smallvec![&"0"], 0)
                ),
                (
                    IndexId::Primary,
                    IndexQuery::new(&["1", "2", "3"], smallvec![&"1"], 0)
                ),
            ],
        };

        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![
                (
                    IndexId::Primary,
                    IndexQuery::new(&["0"], smallvec![&"0"], 1),
                ),
                (
                    IndexId::Primary,
                    IndexQuery::new(&["1", "2", "3"], smallvec![&"1"], 0)
                ),
            ],
        };

        let actual = QueryPlan::new(&schema, &range, &["0"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![
                (
                    IndexId::Primary,
                    IndexQuery::new(&["0"], smallvec![&"0"], 1),
                ),
                (
                    IndexId::Primary,
                    IndexQuery::new(&["1", "2", "3"], smallvec![&"1"], 1),
                ),
            ],
        };

        let actual = QueryPlan::new(&schema, &range, &["0", "1"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![
                (
                    IndexId::Primary,
                    IndexQuery::new(&["0"], smallvec![&"0"], 1),
                ),
                (
                    IndexId::Primary,
                    IndexQuery::new(&["1", "2", "3"], smallvec![&"1"], 2),
                ),
            ],
        };

        let actual = QueryPlan::new(&schema, &range, &["0", "1", "2"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![
                (
                    IndexId::Primary,
                    IndexQuery::new(&["0"], smallvec![&"0"], 1),
                ),
                (
                    IndexId::Primary,
                    IndexQuery::new(&["1", "2", "3"], smallvec![&"1"], 3),
                ),
            ],
        };

        let actual = QueryPlan::new(&schema, &range, &["0", "1", "2", "3"]).expect("plan");
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_query_plan_one_aux_index_one_column_range() {
        let schema = TestTable {
            primary: TestIndex {
                columns: vec!["0", "1", "2", "3", "value"],
            },
            aux: vec![(
                "1".to_string(),
                TestIndex {
                    columns: vec!["1", "0", "2", "3"],
                },
            )],
        };

        let schema = TableSchema::from(schema);

        let mut range = HashMap::new();
        range.insert("1", (2..3).into());

        let expected = QueryPlan {
            indices: smallvec![(
                IndexId::Auxiliary("1"),
                IndexQuery::new(&["1", "0", "2", "3"], smallvec![&"1"], 0),
            )],
        };

        let actual = QueryPlan::new(&schema, &range, &[]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![(
                IndexId::Auxiliary("1"),
                IndexQuery::new(&["1", "0", "2", "3"], smallvec![&"1"], 1),
            )],
        };

        let actual = QueryPlan::new(&schema, &range, &["1"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![(
                IndexId::Auxiliary("1"),
                IndexQuery::new(&["1", "0", "2", "3"], smallvec![&"1"], 2),
            )],
        };

        let actual = QueryPlan::new(&schema, &range, &["1", "0"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![
                (IndexId::Primary, IndexQuery::new(&["0"], smallvec![], 1),),
                (
                    IndexId::Primary,
                    IndexQuery::new(&["1", "2", "3"], smallvec![&"1"], 1),
                )
            ],
        };

        let actual = QueryPlan::new(&schema, &range, &["0", "1"]).expect("plan");
        assert_eq!(expected, actual);

        let actual = QueryPlan::new(&schema, &range, &["3"]);
        assert_eq!(None, actual);
    }

    #[test]
    fn test_query_plan_multi_aux_index() {
        let schema = TestTable {
            primary: TestIndex {
                columns: vec!["0", "1", "2", "value"],
            },
            aux: vec![
                (
                    "1-0-2".to_string(),
                    TestIndex {
                        columns: vec!["1", "0", "2"],
                    },
                ),
                (
                    "1-2-0".to_string(),
                    TestIndex {
                        columns: vec!["1", "2", "0"],
                    },
                ),
                (
                    "2-0-1".to_string(),
                    TestIndex {
                        columns: vec!["2", "0", "1"],
                    },
                ),
                (
                    "2-1-0".to_string(),
                    TestIndex {
                        columns: vec!["2", "1", "0"],
                    },
                ),
            ],
        };

        let schema = TableSchema::from(schema);

        let mut range = HashMap::new();
        range.insert("1", (1..2).into());

        let expected = QueryPlan {
            indices: smallvec![(
                IndexId::Auxiliary("1-0-2"),
                IndexQuery::new(&["1", "0", "2"], smallvec![&"1"], 2),
            ),],
        };

        let actual = QueryPlan::new(&schema, &range, &["1", "0"]).expect("plan");
        assert_eq!(expected, actual);

        let expected = QueryPlan {
            indices: smallvec![(
                IndexId::Auxiliary("1-2-0"),
                IndexQuery::new(&["1", "2", "0"], smallvec![&"1"], 3),
            ),],
        };

        let actual = QueryPlan::new(&schema, &range, &["1", "2", "0"]).expect("plan");
        assert_eq!(expected, actual);
    }
}
