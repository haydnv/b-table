use std::mem;
use std::pin::Pin;

use futures::ready;
use futures::stream::{Fuse, Stream, StreamExt};
use futures::task::{Context, Poll};
use pin_project::pin_project;

use super::index::Key;

#[pin_project]
pub struct GroupBy<K, V> {
    #[pin]
    source: Fuse<K>,
    pending: Option<Key<V>>,
    columns: usize,
}

impl<K: Stream, V> GroupBy<K, V> {
    pub fn new(source: K, columns: usize) -> Self {
        Self {
            source: source.fuse(),
            pending: None,
            columns,
        }
    }
}

impl<K, V, E> Stream for GroupBy<K, V>
where
    K: Stream<Item = Result<Key<V>, E>>,
    V: Clone + PartialEq,
{
    type Item = Result<Key<V>, E>;

    fn poll_next(self: Pin<&mut Self>, cxt: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        Poll::Ready(loop {
            match ready!(this.source.as_mut().poll_next(cxt)) {
                Some(Ok(key)) => match this.pending.as_mut() {
                    Some(pending) if &key[..*this.columns] == &pending[..] => {
                        // no-op
                    }
                    Some(pending) => {
                        let mut group = key[..*this.columns].to_vec();
                        mem::swap(&mut *pending, &mut group);
                        break Some(Ok(group));
                    }
                    None => {
                        *this.pending = Some(key[..*this.columns].to_vec());
                    }
                },
                None => break this.pending.take().map(Ok),
                Some(Err(cause)) => break Some(Err(cause)),
            }
        })
    }
}
