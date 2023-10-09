use std::fmt;

use async_trait::async_trait;
use b_tree::collate::Collate;
use destream::de;
use freqfs::{DirLock, FileLoad};
use futures::TryFutureExt;
use safecast::AsType;

use super::{Node, Range, Schema, TableLock};

struct TableVisitor<S, IS, C, FE> {
    table: TableLock<S, IS, C, FE>,
}

#[async_trait]
impl<S, IS, C, FE> de::Visitor for TableVisitor<S, IS, C, FE>
where
    S: Schema<Index = IS> + Send + Sync + fmt::Debug,
    IS: b_tree::Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    S::Value: de::FromStream<Context = ()>,
    Node<S::Value>: FileLoad + fmt::Debug,
    Range<S::Id, S::Value>: fmt::Debug,
    IS::Error: Send + Sync,
{
    type Value = TableLock<S, IS, C, FE>;

    fn expecting() -> &'static str {
        "a Table"
    }

    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let key_len = self.table.schema().key().len();

        let mut table = self.table.write().await;

        while let Some(mut row) = seq.next_element::<Vec<S::Value>>(()).await? {
            if row.len() >= key_len {
                let values = row.drain(key_len..).collect();
                let key = row;
                table.upsert(key, values).map_err(de::Error::custom).await?;
            } else {
                return Err(de::Error::invalid_length(
                    row.len(),
                    format!("a row of a table with schema {:?}", self.table.schema()),
                ));
            }
        }

        Ok(self.table)
    }
}

#[async_trait]
impl<S, IS, C, FE> de::FromStream for TableLock<S, IS, C, FE>
where
    S: Schema<Index = IS> + Send + Sync + fmt::Debug,
    IS: b_tree::Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    S::Value: de::FromStream<Context = ()>,
    Node<S::Value>: FileLoad + fmt::Debug,
    Range<S::Id, S::Value>: fmt::Debug,
    IS::Error: Send + Sync,
{
    type Context = (S, C, DirLock<FE>);

    async fn from_stream<D: de::Decoder>(
        context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        let (schema, collator, dir) = context;
        let table = TableLock::create(schema, collator, dir).map_err(de::Error::custom)?;
        decoder.decode_seq(TableVisitor { table }).await
    }
}
