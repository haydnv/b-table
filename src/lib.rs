pub use b_tree::Collator;
pub use index::collate;
pub use schema::*;
pub use table::*;

mod group;
mod index;
mod schema;
#[cfg(feature = "stream")]
mod stream;
mod table;

/// A node in an [`Table`] index
pub type Node<V> = b_tree::Node<Vec<Key<V>>>;
