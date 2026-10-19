use super::*;
use temp_testdir::TempDir;

#[tokio::test]
async fn can_construct() {
    let td = TempDir::default();
    let store: SurrealKvStore = SurrealKvStore::builder()
        .with_path(td.as_ref().into())
        .try_into()
        .unwrap();
    let key = StoreKey::new("prefix/obj").unwrap();
    let value = b"Hello, world!";
    store.set(&key, Bytes::from_static(value)).await.unwrap();
    let v2 = store.get(&key).await.unwrap().unwrap();
    assert_eq!(value, v2.as_ref());

    let v3 = store
        .get(&StoreKey::new("prefix/obj2").unwrap())
        .await
        .unwrap();
    assert!(v3.is_none());

    let keys = store.list().await.unwrap();
    assert_eq!(keys, vec![key.clone()]);

    let pref_keys = store
        .list_prefix(&StorePrefix::new("prefix/").unwrap())
        .await
        .unwrap();
    assert_eq!(pref_keys, vec![key.clone()]);
}
