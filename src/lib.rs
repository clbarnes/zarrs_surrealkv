#[doc = include_str!("../README.md")]
use futures::stream::{self, StreamExt};
pub use surrealkv;
use surrealkv::{InternalKeyRef, LSMIterator, Mode, ReadOptions, Transaction};
pub use zarrs_storage;
use zarrs_storage::{
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits,
    AsyncWritableStorageTraits, Bytes, MaybeBytes, OffsetBytesIterator, StorageError, StoreKey,
    StoreKeys, StoreKeysPrefixes, StorePrefix, StorePrefixes,
    byte_range::{ByteRangeIterator, extract_byte_ranges},
};

#[cfg(test)]
mod tests;

fn storage_err(e: impl std::error::Error) -> StorageError {
    StorageError::Other(e.to_string())
}

/// Smallest byte string greater than all strings starting with `prefix`,
/// or `None` if no such bound exists.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

#[derive(Clone)]
pub struct SurrealKvStore {
    tree: surrealkv::Tree,
}

impl From<surrealkv::Tree> for SurrealKvStore {
    fn from(tree: surrealkv::Tree) -> Self {
        Self { tree }
    }
}

impl TryFrom<surrealkv::TreeBuilder> for SurrealKvStore {
    type Error = StorageError;

    fn try_from(builder: surrealkv::TreeBuilder) -> Result<Self, Self::Error> {
        let tree = builder.build().map_err(storage_err)?;
        Ok(Self { tree })
    }
}

impl SurrealKvStore {
    pub fn builder() -> surrealkv::TreeBuilder {
        surrealkv::TreeBuilder::default()
    }

    pub async fn close(self) -> Result<(), StorageError> {
        self.tree.close().await.map_err(storage_err)
    }

    fn kv_key<'a>(&self, key: &'a StoreKey) -> &'a [u8] {
        key.as_str().as_bytes()
    }

    fn read_tx(&self) -> Result<Transaction, StorageError> {
        self.tree
            .begin_with_mode(Mode::ReadOnly)
            .map_err(storage_err)
    }

    fn write_tx(&self) -> Result<Transaction, StorageError> {
        self.tree
            .begin_with_mode(Mode::ReadWrite)
            .map_err(storage_err)
    }

    fn scan_prefix_keys(
        &self,
        tx: &Transaction,
        prefix: &StorePrefix,
        mut f: impl FnMut(&[u8]) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        self.scan_prefix_inner(tx, prefix, |k, _it| f(k.user_key()))
    }

    /// WARNING: this reads every value
    fn scan_prefix_key_values(
        &self,
        tx: &Transaction,
        prefix: &StorePrefix,
        mut f: impl FnMut(&[u8], Vec<u8>) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        self.scan_prefix_inner(tx, prefix, |k, it| {
            f(k.user_key(), it.value().map_err(storage_err)?)
        })
    }

    fn scan_prefix_inner(
        &self,
        tx: &Transaction,
        prefix: &StorePrefix,
        mut f: impl FnMut(&InternalKeyRef, &dyn LSMIterator) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let lower = prefix.as_str().as_bytes();
        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(prefix_upper_bound(lower));
        opts.set_iterate_lower_bound(Some(lower.to_vec()));
        let mut iter = tx.range_with_options(&opts).map_err(storage_err)?;
        iter.seek_first().map_err(storage_err)?;
        while iter.valid() {
            let key_ref = iter.key();
            if !key_ref.is_tombstone() && !key_ref.is_hard_delete_marker() && !key_ref.is_replace()
            {
                f(&key_ref, &iter)?;
            }
            iter.next().map_err(storage_err)?;
        }
        Ok(())
    }

    fn list_inner(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let tx = self.read_tx()?;
        let mut keys = Vec::new();
        self.scan_prefix_keys(&tx, prefix, |k| {
            let s = String::from_utf8(k.to_vec()).expect("keys are valid UTF-8");
            let sk = StoreKey::new(s).expect("keys are valid store keys");
            keys.push(sk);
            Ok(())
        })?;
        Ok(keys)
    }
}

#[async_trait::async_trait]
impl AsyncReadableStorageTraits for SurrealKvStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let tx = self.read_tx()?;
        let value = tx.get(self.kv_key(key)).map_err(storage_err)?;
        Ok(value.map(Bytes::from))
    }

    async fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
        let Some(value) = self.get(key).await? else {
            return Ok(None);
        };
        let parts = extract_byte_ranges(&value, byte_ranges)?;
        Ok(Some(
            stream::iter(parts.into_iter().map(|p| Ok(Bytes::from(p)))).boxed(),
        ))
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let tx = self.read_tx()?;
        let value = tx.get(self.kv_key(key)).map_err(storage_err)?;
        Ok(value.map(|v| v.len() as u64))
    }

    fn supports_get_partial(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl AsyncListableStorageTraits for SurrealKvStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.list_inner(&StorePrefix::root())
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.list_inner(prefix)
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let tx = self.read_tx()?;
        let mut keys = Vec::new();
        let mut prefixes: StorePrefixes = Vec::new();
        let prefix_bytes = prefix.as_str().as_bytes();
        self.scan_prefix_keys(&tx, prefix, |key| {
            let rest = &key[prefix_bytes.len()..];

            let Some(split) = rest.iter().position(|b| *b == b'/') else {
                keys.push(StoreKey::new(
                    String::from_utf8(key.to_vec()).expect("valid UTF-8"),
                )?);
                return Ok(());
            };

            let next = &rest[..split + 1];
            if let Some(last) = prefixes.last() {
                let last_str = last.as_str().as_bytes();
                if &last_str[prefix_bytes.len()..last_str.len() - 1] == next {
                    return Ok(());
                }
            }

            let key_len = prefix_bytes.len() + next.len() + 1;
            let buf = key[..key_len].to_vec();
            let s = String::from_utf8(buf).expect("should be valid UTF-8");

            prefixes.push(StorePrefix::new(s)?);
            Ok(())
        })?;
        Ok(StoreKeysPrefixes::new(keys, prefixes))
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let tx = self.read_tx()?;
        let mut size = 0u64;
        self.scan_prefix_key_values(&tx, prefix, |_, v| {
            size += v.len() as u64;
            Ok(())
        })?;
        Ok(size)
    }
}

#[async_trait::async_trait]
impl AsyncWritableStorageTraits for SurrealKvStore {
    async fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        let mut tx = self.write_tx()?;
        tx.set(self.kv_key(key), value.as_ref())
            .map_err(storage_err)?;
        tx.commit().await.map_err(storage_err)
    }

    async fn set_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        offset_values: OffsetBytesIterator<'a>,
    ) -> Result<(), StorageError> {
        let kv_key = self.kv_key(key);
        let mut tx = self.write_tx()?;
        let mut data = tx
            .get_for_update(kv_key)
            .map_err(storage_err)?
            .unwrap_or_default();
        for (offset, bytes) in offset_values {
            let start = usize::try_from(offset).map_err(|e| StorageError::Other(e.to_string()))?;
            let end = start + bytes.len();
            if data.len() < end {
                data.resize(end, 0);
            }
            data[start..end].copy_from_slice(&bytes);
        }
        tx.set(kv_key, data).map_err(storage_err)?;
        tx.commit().await.map_err(storage_err)
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        let mut tx = self.write_tx()?;
        tx.delete(self.kv_key(key)).map_err(storage_err)?;
        tx.commit().await.map_err(storage_err)
    }

    async fn erase_many(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        let mut tx = self.write_tx()?;
        for key in keys {
            tx.delete(self.kv_key(key)).map_err(storage_err)?;
        }
        tx.commit().await.map_err(storage_err)
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        let read_tx = self.read_tx()?;
        let mut write_tx = self.write_tx()?;
        self.scan_prefix_keys(&read_tx, prefix, |k| {
            write_tx.delete(k).map_err(storage_err)
        })?;
        drop(read_tx);
        write_tx.commit().await.map_err(storage_err)
    }

    fn supports_set_partial(&self) -> bool {
        false
    }
}
