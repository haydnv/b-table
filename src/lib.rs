pub use b_tree::{collate, Collator};
pub use schema::*;
pub use table::*;

mod schema;
#[cfg(feature = "stream")]
mod stream;
mod table;

/// A node in a [`Table`] index
pub type Node<V> = b_tree::Node<Vec<Vec<V>>>;
