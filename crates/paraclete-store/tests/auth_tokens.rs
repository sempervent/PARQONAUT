//! Auth token hashing and lifecycle.

use paraclete_store::SqliteScanStore;
use paraclete_types::AuthRole;

fn sqlite_url() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t.sqlite");
    std::fs::File::create(&p).unwrap();
    let abs = p.canonicalize().unwrap();
    (dir, format!("sqlite://{}", abs.display()))
}

#[tokio::test]
async fn verify_rejects_unknown_token() {
    let (_dir, url) = sqlite_url();
    let store = SqliteScanStore::connect(&url).await.unwrap();
    assert!(store.verify_bearer_token("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn insert_verify_roundtrip() {
    let (_dir, url) = sqlite_url();
    let store = SqliteScanStore::connect(&url).await.unwrap();
    let id = store.insert_auth_token("a", "secret-one", AuthRole::Operator).await.unwrap();
    let p = store.verify_bearer_token("secret-one").await.unwrap().expect("principal");
    assert_eq!(p.token_id, id);
    assert_eq!(p.label, "a");
    assert_eq!(p.role, AuthRole::Operator);
}

#[tokio::test]
async fn disabled_token_rejected() {
    let (_dir, url) = sqlite_url();
    let store = SqliteScanStore::connect(&url).await.unwrap();
    let id = store.insert_auth_token("b", "secret-two", AuthRole::Reader).await.unwrap();
    assert!(store.verify_bearer_token("secret-two").await.unwrap().is_some());
    store.disable_auth_token(id).await.unwrap();
    assert!(store.verify_bearer_token("secret-two").await.unwrap().is_none());
}

#[tokio::test]
async fn create_auth_token_hashes_only_and_lists_metadata() {
    let (_dir, url) = sqlite_url();
    let store = SqliteScanStore::connect(&url).await.unwrap();
    let (id, secret) =
        store.create_auth_token("from-api", AuthRole::Operator, Some("note")).await.unwrap();
    assert!(secret.starts_with("plc_"));
    let p = store.verify_bearer_token(&secret).await.unwrap().expect("ok");
    assert_eq!(p.token_id, id);

    let rows = store.list_auth_tokens().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].label, "from-api");
    assert_eq!(rows[0].note.as_deref(), Some("note"));
    assert!(rows[0].token_prefix.as_ref().unwrap().len() <= 12);
}

#[tokio::test]
async fn verify_sets_last_used_at() {
    let (_dir, url) = sqlite_url();
    let store = SqliteScanStore::connect(&url).await.unwrap();
    let id = store.insert_auth_token("lu", "secret-lu", AuthRole::Reader).await.unwrap();
    assert!(store.get_auth_token_summary(id).await.unwrap().unwrap().last_used_at.is_none());
    store.verify_bearer_token("secret-lu").await.unwrap();
    let lu = store
        .get_auth_token_summary(id)
        .await
        .unwrap()
        .expect("row")
        .last_used_at
        .expect("last_used_at after auth");
    store.verify_bearer_token("secret-lu").await.unwrap();
    let lu2 = store
        .get_auth_token_summary(id)
        .await
        .unwrap()
        .expect("row")
        .last_used_at
        .expect("last_used_at");
    assert!(lu2 >= lu);
}

#[tokio::test]
async fn rotate_disables_old_and_mints_new_secret() {
    let (_dir, url) = sqlite_url();
    let store = SqliteScanStore::connect(&url).await.unwrap();
    let (old_id, old_secret) = store.create_auth_token("rot", AuthRole::Admin, None).await.unwrap();
    let (new_id, new_secret) = store.rotate_auth_token(old_id).await.unwrap();
    assert_ne!(old_id, new_id);
    assert!(new_secret.starts_with("plc_"));
    assert!(store.verify_bearer_token(&old_secret).await.unwrap().is_none());
    let p = store.verify_bearer_token(&new_secret).await.unwrap().expect("new works");
    assert_eq!(p.token_id, new_id);

    let old_meta = store.get_auth_token_summary(old_id).await.unwrap().expect("old row");
    assert!(old_meta.disabled_at.is_some());
    assert_eq!(old_meta.replaced_by_token_id, Some(new_id));

    let new_meta = store.get_auth_token_summary(new_id).await.unwrap().expect("new row");
    assert!(new_meta.disabled_at.is_none());
    assert!(new_meta.replaced_by_token_id.is_none());
}
