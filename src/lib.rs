pub use b_tree::{collate, Collator};
pub use schema::*;
pub use table::*;

mod plan;
mod schema;
#[cfg(feature = "stream")]
mod stream;
mod table;

const INDEX_STACK_SIZE: usize = 16;

/// The maximum number of values in a stack-allocated [`Row`]
pub const ROW_STACK_SIZE: usize = 32;

/// A node in a [`Table`] index
pub type Node<V> = b_tree::Node<Vec<Vec<V>>>;

type Columns<'a, K> = smallvec::SmallVec<[&'a K; ROW_STACK_SIZE]>;

type IndexStack<T> = smallvec::SmallVec<[T; INDEX_STACK_SIZE]>;
