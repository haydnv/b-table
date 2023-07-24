use std::sync::Arc;
use std::{fmt, io};

use b_tree::collate::Collate;
use b_tree::{BTree, BTreeLock};
use freqfs::{DirDeref, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::{FutureExt, Stream};
use safecast::AsType;

use super::schema::IndexSchema;
use super::Node;

pub use b_tree::collate;
pub use b_tree::{Collator, Key, Range};

pub type IndexReadGuard<S, C, FE> = Index<S, C, Arc<DirReadGuardOwned<FE>>>;

pub type IndexWriteGuard<S, C, FE> = Index<S, C, DirWriteGuardOwned<FE>>;

pub struct IndexLock<S, C, FE> {
    btree: BTreeLock<S, C, FE>,
}

impl<S, C, FE> Clone for IndexLock<S, C, FE> {
    fn clone(&self) -> Self {
        Self {
            btree: self.btree.clone(),
        }
    }
}

impl<S, C, FE> IndexLock<S, C, FE> {
    pub fn collator(&self) -> &Arc<Collator<C>> {
        self.btree.collator()
    }
}

impl<S, C, FE> IndexLock<S, C, FE>
where
    S: IndexSchema,
    FE: AsType<Node<S::Value>> + Send + Sync,
    Node<S::Value>: FileLoad,
{
    pub fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        BTreeLock::create(schema, collator, dir).map(|btree| Self { btree })
    }

    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        BTreeLock::load(schema, collator, dir).map(|btree| Self { btree })
    }
}

impl<S, C, FE> IndexLock<S, C, FE>
where
    FE: Send + Sync,
{
    pub async fn into_read(self) -> IndexReadGuard<S, C, FE> {
        self.btree
            .into_read()
            .map(|btree| IndexReadGuard { btree })
            .await
    }

    pub async fn read(&self) -> IndexReadGuard<S, C, FE> {
        self.btree
            .read()
            .map(|btree| IndexReadGuard { btree })
            .await
    }

    pub async fn into_write(self) -> IndexWriteGuard<S, C, FE> {
        self.btree
            .into_write()
            .map(|btree| IndexWriteGuard { btree })
            .await
    }

    pub async fn write(&self) -> IndexWriteGuard<S, C, FE> {
        self.btree
            .write()
            .map(|btree| IndexWriteGuard { btree })
            .await
    }
}

pub struct Index<S, C, G> {
    btree: BTree<S, C, G>,
}

impl<S, C, G> Clone for Index<S, C, G>
where
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            btree: self.btree.clone(),
        }
    }
}

impl<S, C, G> Index<S, C, G>
where
    S: IndexSchema,
    C: Collate<Value = S::Value>,
{
    pub fn schema(&self) -> &S {
        self.btree.schema()
    }
}

impl<S, C, FE, G> Index<S, C, G>
where
    S: IndexSchema,
    C: Collate<Value = S::Value>,
    FE: AsType<Node<S::Value>> + Send + Sync,
    G: DirDeref<Entry = FE>,
    Node<S::Value>: FileLoad + fmt::Debug,
{
    pub async fn contains(&self, key: &Key<S::Value>) -> Result<bool, io::Error> {
        self.btree.contains(key).await
    }

    pub async fn count(&self, range: &Range<S::Value>) -> Result<u64, io::Error> {
        self.btree.count(range).await
    }

    pub async fn first(&self, range: &Range<S::Value>) -> Result<Option<Key<S::Value>>, io::Error> {
        self.btree.first(range).await
    }

    pub async fn is_empty(&self, range: &Range<S::Value>) -> Result<bool, io::Error> {
        self.btree.is_empty(range).await
    }
}

impl<S, C, FE, G> Index<S, C, G>
where
    S: IndexSchema,
    C: Collate<Value = S::Value> + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<S::Value>: FileLoad + fmt::Debug,
{
    pub fn keys<R: Into<Arc<Range<S::Value>>>>(
        self,
        range: R,
        reverse: bool,
    ) -> impl Stream<Item = Result<Key<S::Value>, io::Error>> + Unpin + Send + Sized {
        self.btree.keys(range, reverse)
    }
}

impl<S: IndexSchema, C: Collate<Value = S::Value>, G> fmt::Debug for Index<S, C, G> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "index on columns {:?}", self.btree.schema().columns())
    }
}

impl<S, C, FE> IndexWriteGuard<S, C, FE> {
    pub fn downgrade(self) -> IndexReadGuard<S, C, FE> {
        IndexReadGuard {
            btree: self.btree.downgrade(),
        }
    }
}

impl<S, C, FE> IndexWriteGuard<S, C, FE>
where
    S: IndexSchema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    Node<S::Value>: FileLoad,
{
    pub async fn delete(&mut self, key: &Key<S::Value>) -> Result<bool, S::Error> {
        self.btree.delete(key).await
    }

    pub async fn delete_all<G>(&mut self, other: Index<S, C, G>) -> Result<(), S::Error>
    where
        G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        self.btree.delete_all(other.btree).await
    }

    pub async fn insert(&mut self, key: Key<S::Value>) -> Result<bool, S::Error> {
        self.btree.insert(key).await
    }

    pub async fn merge<G>(&mut self, other: Index<S, C, G>) -> Result<(), S::Error>
    where
        G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        self.btree.merge(other.btree).await
    }

    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        self.btree.truncate().await
    }
}
