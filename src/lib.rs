pub use b_tree::Collator;
pub use index::collate;
pub use schema::*;
pub use table::*;

mod index;
mod schema;
#[cfg(feature = "stream")]
mod stream;
mod table;

/// A node in a [`Table`] index
pub type Node<V> = b_tree::Node<Vec<Key<V>>>;
