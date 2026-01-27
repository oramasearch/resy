use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::SharedCredentialsProvider;
use chrono::{DateTime, Utc};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Connection, Row, SqliteConnection};
use std::path::{Path, PathBuf};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::ChangeStream;

const DEFAULT_BATCH_SIZE: i32 = 1000;

/// Represents an S3 object with its metadata
#[derive(Clone, Debug, PartialEq)]
pub struct S3Object {
    /// S3 object key (path)
    pub key: String,
    /// ETag for version tracking
    pub etag: String,
    /// Object size in bytes
    pub size: i64,
    /// Last modification timestamp
    pub last_modified: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CompactS3Object {
    etag: String,
    size: i64,
    last_modified: i64,
}

#[derive(Debug, PartialEq)]
pub enum Change {
    Added(S3Object),
    Modified { old: S3Object, new: S3Object },
    Deleted(S3Object),
}

#[derive(Debug, thiserror::Error)]
pub enum S3BuilderError {
    #[error("Missing required field: {0}")]
    MissingField(&'static str),
    #[error("Invalid batch size: {0}. Must be at least 1")]
    InvalidBatchSize(i32),
}

#[derive(Zeroize, ZeroizeOnDrop)]
struct S3Credentials {
    access_key_id: SecretString,
    secret_access_key: SecretString,
}

pub struct S3Builder {
    bucket: Option<String>,
    region: Option<String>,
    credentials: Option<S3Credentials>,
    endpoint_url: Option<String>,
    batch_size: Option<i32>,
}

impl Default for S3Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl S3Builder {
    pub fn new() -> Self {
        Self {
            bucket: None,
            region: None,
            credentials: None,
            endpoint_url: None,
            batch_size: Some(DEFAULT_BATCH_SIZE),
        }
    }

    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.credentials = Some(S3Credentials {
            access_key_id: SecretString::new(access_key_id.into().into()),
            secret_access_key: SecretString::new(secret_access_key.into().into()),
        });
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint.into());
        self
    }

    pub fn batch_size(mut self, size: i32) -> Self {
        self.batch_size = Some(size);
        self
    }

    pub async fn build(mut self) -> Result<S3, S3BuilderError> {
        let bucket = self
            .bucket
            .take()
            .ok_or(S3BuilderError::MissingField("bucket"))?;
        let region = self
            .region
            .take()
            .ok_or(S3BuilderError::MissingField("region"))?;
        let credentials = self
            .credentials
            .take()
            .ok_or(S3BuilderError::MissingField("credentials"))?;

        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        if batch_size <= 0 {
            return Err(S3BuilderError::InvalidBatchSize(batch_size));
        }

        let credentials = Credentials::new(
            credentials.access_key_id.expose_secret(),
            credentials.secret_access_key.expose_secret(),
            None,
            None,
            "resy",
        );

        let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(Region::new(region))
            .behavior_version(BehaviorVersion::latest());

        if let Some(endpoint_url) = &self.endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_config = s3_config_builder.build();
        let s3_client = Client::from_conf(s3_config);

        Ok(S3 {
            client: s3_client,
            bucket,
            batch_size,
        })
    }
}

/// S3 client for monitoring bucket changes
///
/// # Example
/// ```no_run
/// # use resy::s3::S3;
/// # use tokio_stream::StreamExt;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let s3 = S3::builder()
///     .bucket("my-bucket")
///     .region("us-west-2")
///     .credentials("key", "secret")
///     .build()
///     .await?;
///     
/// let mut changes = s3.stream_changes("").await.unwrap();
/// while let Some(change) = changes.next().await {
///     // Handle change
/// #   break;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct S3 {
    client: Client,
    bucket: String,
    batch_size: i32,
}

impl S3 {
    /// Create a new S3Builder to configure and build an S3 client
    pub fn builder() -> S3Builder {
        S3Builder::new()
    }

    /// Create an S3 instance from an existing AWS SDK S3 client
    pub fn from_client(client: Client, bucket: String, batch_size: Option<i32>) -> Self {
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        Self {
            client,
            bucket,
            batch_size,
        }
    }

    // Create the sqlite db, it is acquired with Exclusive lock, so only one process can use
    // the db at time. A transaction is not needed.
    pub async fn create_state_db(db_path: &Path) -> Result<SqliteConnection, sqlx::Error> {
        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .locking_mode(sqlx::sqlite::SqliteLockingMode::Exclusive)
            .disable_statement_logging();

        let mut pool = SqliteConnection::connect_with(&options).await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS object_state (
                key TEXT PRIMARY KEY,
                etag TEXT NOT NULL,
                size INTEGER NOT NULL,
                last_modified INTEGER NOT NULL
            )",
        )
        .execute(&mut pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .execute(&mut pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_etag ON object_state(etag)")
            .execute(&mut pool)
            .await?;

        Ok(pool)
    }

    /// Stream changes using the configured or auto-generated database path
    pub async fn stream_changes<P: Into<PathBuf>>(
        &self,
        db_path: P,
    ) -> Result<ChangeStream<Change>, crate::ResyError> {
        let actual_db_path = db_path.into();
        self.stream_diff_and_update(&actual_db_path).await
    }

    pub async fn stream_diff_and_update(
        &self,
        db_path: &Path,
    ) -> Result<ChangeStream<Change>, crate::ResyError> {
        let db_path = db_path.to_path_buf();
        let bucket = self.bucket.clone();
        let client = self.client.clone();
        let batch_size = self.batch_size;

        let mut conn = Self::create_state_db(&db_path).await.map_err(|e| {
            format!(
                "Failed to create state database at {}: {}",
                db_path.display(),
                e
            )
        })?;

        // Use channel of one item to prevent marking items as seen
        // before the consumer consumed it.
        // This ensures we never lose items if the process crashes.
        let (sender, receiver) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            // util to send err
            let send_err = |e: crate::ResyError| async {
                let _ = sender.send(Err(e)).await;
            };

            if let Err(e) = Self::setup_temp_tracking_column(&mut conn).await {
                send_err(e.into()).await;
                return;
            }

            // Handle created and modified objects by syncing from S3 to local database
            if let Err(e) =
                Self::sync_s3_objects(&client, &bucket, batch_size, &mut conn, &sender).await
            {
                send_err(e).await;
                return;
            }

            // Handle deleted objects, objects present in the local database but not seen
            if let Err(e) = Self::handle_deleted_objects(&mut conn, &sender).await {
                send_err(e).await;
                return;
            }

            if let Err(e) = Self::cleanup_temp_column(&mut conn).await {
                send_err(e.into()).await;
                return;
            }

            if let Err(e) = Self::update_last_sync_metadata(&mut conn).await {
                send_err(e.into()).await;
            }
        });

        Ok(tokio_stream::wrappers::ReceiverStream::new(receiver))
    }

    fn determine_change(
        current_obj: &S3Object,
        previous_state: Option<CompactS3Object>,
    ) -> Option<Change> {
        match previous_state {
            None => Some(Change::Added(current_obj.clone())),
            Some(prev_obj) if prev_obj.etag != current_obj.etag => Some(Change::Modified {
                old: Self::compact_to_s3_object(&current_obj.key, &prev_obj),
                new: current_obj.clone(),
            }),
            _ => None,
        }
    }

    fn parse_aws_object(obj: &aws_sdk_s3::types::Object) -> Option<S3Object> {
        Some(S3Object {
            key: obj.key()?.to_string(),
            etag: obj.e_tag()?.to_string(),
            size: obj.size()?,
            last_modified: {
                let lm = obj.last_modified()?;
                DateTime::from_timestamp(lm.secs(), lm.subsec_nanos())
                    .unwrap_or_default()
                    .with_timezone(&Utc)
            },
        })
    }

    pub fn compact_to_s3_object(key: &str, compact: &CompactS3Object) -> S3Object {
        S3Object {
            key: key.to_string(),
            etag: compact.etag.clone(),
            size: compact.size,
            last_modified: DateTime::from_timestamp(compact.last_modified, 0)
                .unwrap_or_default()
                .with_timezone(&Utc),
        }
    }

    async fn insert_object_state(
        conn: &mut sqlx::SqliteConnection,
        obj: &S3Object,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT OR REPLACE INTO object_state (key, etag, size, last_modified, temp_seen) \
             VALUES (?1, ?2, ?3, ?4, 1)",
        )
        .bind(&obj.key)
        .bind(&obj.etag)
        .bind(obj.size)
        .bind(obj.last_modified.timestamp())
        .execute(conn)
        .await?;
        Ok(())
    }

    async fn mark_object_seen(
        conn: &mut sqlx::SqliteConnection,
        key: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE object_state SET temp_seen = 1 WHERE key = ?1")
            .bind(key)
            .execute(conn)
            .await?;
        Ok(())
    }

    /// Setup the temporary tracking column for detecting deletions
    async fn setup_temp_tracking_column(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        sqlx::query("ALTER TABLE object_state ADD COLUMN temp_seen INTEGER DEFAULT 0")
            .execute(&mut *conn)
            .await
            .ok();

        // Reset all to unseen (required for SQLite < 3.35.0 that doesn't support DROP COLUMN)
        sqlx::query("UPDATE object_state SET temp_seen = 0")
            .execute(conn)
            .await?;

        Ok(())
    }

    async fn cleanup_temp_column(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        // SQLite doesn't support DROP COLUMN before version 3.35.0
        sqlx::query("ALTER TABLE object_state DROP COLUMN IF EXISTS temp_seen")
            .execute(conn)
            .await
            .ok();
        Ok(())
    }

    /// Fetch previous state for an object from the database
    async fn fetch_previous_state(
        conn: &mut SqliteConnection,
        key: &str,
    ) -> Result<Option<CompactS3Object>, sqlx::Error> {
        match sqlx::query("SELECT etag, size, last_modified FROM object_state WHERE key = ?1")
            .bind(key)
            .fetch_optional(conn)
            .await?
        {
            Some(row) => Ok(Some(CompactS3Object {
                etag: row.get(0),
                size: row.get(1),
                last_modified: row.get(2),
            })),
            None => Ok(None),
        }
    }

    /// Process a single S3 object: determine change, update database state
    async fn process_s3_object(
        conn: &mut SqliteConnection,
        current_obj: &S3Object,
    ) -> Result<Option<Change>, sqlx::Error> {
        let previous_state = Self::fetch_previous_state(conn, &current_obj.key).await?;
        let change = Self::determine_change(current_obj, previous_state.clone());

        match previous_state {
            None => {
                // New object: insert into DB and mark as seen
                Self::insert_object_state(conn, current_obj).await?;
            }
            Some(_) => {
                // Existing object: mark as seen
                Self::mark_object_seen(conn, &current_obj.key).await?;

                // If modified, update the state
                if change.is_some() {
                    Self::insert_object_state(conn, current_obj).await?;
                }
            }
        }

        Ok(change)
    }

    /// Fetch all objects that were not seen during sync (deleted from S3)
    async fn fetch_deleted_objects(
        conn: &mut SqliteConnection,
    ) -> Result<Vec<(String, CompactS3Object)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT key, etag, size, last_modified FROM object_state WHERE temp_seen = 0",
        )
        .fetch_all(conn)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>(0),
                    CompactS3Object {
                        etag: row.get(1),
                        size: row.get(2),
                        last_modified: row.get(3),
                    },
                )
            })
            .collect())
    }

    /// Delete an object from the database
    async fn delete_object_state(
        conn: &mut SqliteConnection,
        key: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM object_state WHERE key = ?1")
            .bind(key)
            .execute(conn)
            .await?;
        Ok(())
    }

    async fn update_last_sync_metadata(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_updated', ?1)")
            .bind(Utc::now().timestamp())
            .execute(conn)
            .await?;
        Ok(())
    }

    /// Sync all objects from S3 to the database, yielding changes to the channel
    /// Commits after each batch for crash recovery. If the process crashes, it will
    /// restart from the beginning, but already-processed items will be skipped as
    /// their etag matches (no change detected).
    async fn sync_s3_objects(
        client: &Client,
        bucket: &str,
        batch_size: i32,
        conn: &mut SqliteConnection,
        sender: &tokio::sync::mpsc::Sender<Result<Change, crate::ResyError>>,
    ) -> Result<(), crate::ResyError> {
        let mut continuation_token: Option<String> = None;

        loop {
            let mut db_tx = conn.begin().await?;

            let mut request = client.list_objects_v2().bucket(bucket).max_keys(batch_size);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            for obj in response.contents() {
                let Some(current_obj) = Self::parse_aws_object(obj) else {
                    continue;
                };

                let change = Self::process_s3_object(&mut db_tx, &current_obj).await?;

                if let Some(change) = change
                    && sender.send(Ok(change)).await.is_err()
                {
                    return Ok(());
                }
            }

            db_tx.commit().await?;

            if response.is_truncated().unwrap_or(false) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn handle_deleted_objects(
        conn: &mut SqliteConnection,
        tx: &tokio::sync::mpsc::Sender<Result<Change, crate::ResyError>>,
    ) -> Result<(), crate::ResyError> {
        let deleted_rows = Self::fetch_deleted_objects(conn).await?;

        for (key, prev_obj) in deleted_rows {
            let change = Change::Deleted(Self::compact_to_s3_object(&key, &prev_obj));

            if tx.send(Ok(change)).await.is_err() {
                return Ok(());
            }

            Self::delete_object_state(conn, &key).await?;
        }

        Ok(())
    }
}

impl crate::DataSource for S3 {
    type Change = Change;

    async fn stream_changes<P: Into<PathBuf> + Send>(
        &self,
        db_path: P,
    ) -> Result<ChangeStream<Self::Change>, crate::ResyError> {
        S3::stream_changes(self, db_path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn check_send_sync<T: Send + Sync>(_: T) {}
    #[tokio::test]
    async fn test_sync_send() {
        let s3 = S3::builder()
            .bucket("my-bucket")
            .region("us-west-2")
            .credentials("key", "secret")
            .build()
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let changes = s3.stream_changes(db_path).await.unwrap();
        check_send_sync(changes);
    }

    #[tokio::test]
    async fn test_create_state_db() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let mut pool = S3::create_state_db(db_path).await.unwrap();

        let tables: Vec<String> = sqlx::query("SELECT name FROM sqlite_master WHERE type='table'")
            .fetch_all(&mut pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect();

        assert!(tables.contains(&"object_state".to_string()));
        assert!(tables.contains(&"metadata".to_string()));

        let index_exists =
            sqlx::query("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_etag'")
                .fetch_optional(&mut pool)
                .await
                .unwrap()
                .is_some();
        assert!(index_exists);
    }

    #[tokio::test]
    async fn test_builder_missing_bucket() {
        let result = S3::builder()
            .region("us-west-2")
            .credentials("test-key", "test-secret")
            .build()
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3BuilderError::MissingField("bucket")));
    }

    #[tokio::test]
    async fn test_builder_missing_region() {
        let result = S3::builder()
            .bucket("test-bucket")
            .credentials("test-key", "test-secret")
            .build()
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3BuilderError::MissingField("region")));
    }

    #[tokio::test]
    async fn test_builder_missing_credentials() {
        let result = S3::builder()
            .bucket("test-bucket")
            .region("us-west-2")
            .build()
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3BuilderError::MissingField("credentials")));
    }

    #[tokio::test]
    async fn test_builder_invalid_batch_size_negative() {
        let result = S3::builder()
            .bucket("test-bucket")
            .region("us-west-2")
            .credentials("test-key", "test-secret")
            .batch_size(-1)
            .build()
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3BuilderError::InvalidBatchSize(-1)));
    }

    #[tokio::test]
    async fn test_builder_invalid_batch_size_zero() {
        let result = S3::builder()
            .bucket("test-bucket")
            .region("us-west-2")
            .credentials("test-key", "test-secret")
            .batch_size(0)
            .build()
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3BuilderError::InvalidBatchSize(0)));
    }

    fn create_test_s3_object(key: &str, etag: &str, size: i64, timestamp: i64) -> S3Object {
        S3Object {
            key: key.to_string(),
            etag: etag.to_string(),
            size,
            last_modified: chrono::DateTime::from_timestamp(timestamp, 0)
                .unwrap()
                .with_timezone(&chrono::Utc),
        }
    }

    #[test]
    fn test_determine_change_added() {
        let obj = create_test_s3_object("new-file.txt", "etag1", 100, 1609459200);
        let change = S3::determine_change(&obj, None);

        assert!(matches!(change, Some(Change::Added(_))));
        if let Some(Change::Added(added_obj)) = change {
            assert_eq!(added_obj.key, "new-file.txt");
            assert_eq!(added_obj.etag, "etag1");
            assert_eq!(added_obj.size, 100);
        }
    }

    #[test]
    fn test_determine_change_modified() {
        let obj = create_test_s3_object("file.txt", "etag2", 200, 1609459300);
        let prev = CompactS3Object {
            etag: "etag1".to_string(),
            size: 100,
            last_modified: 1609459200,
        };

        let change = S3::determine_change(&obj, Some(prev));

        assert!(matches!(change, Some(Change::Modified { .. })));
        if let Some(Change::Modified { old, new }) = change {
            assert_eq!(old.etag, "etag1");
            assert_eq!(old.size, 100);
            assert_eq!(new.etag, "etag2");
            assert_eq!(new.size, 200);
            assert_eq!(new.key, "file.txt");
        }
    }

    #[test]
    fn test_determine_change_unchanged() {
        let obj = create_test_s3_object("file.txt", "etag1", 100, 1609459200);
        let prev = CompactS3Object {
            etag: "etag1".to_string(),
            size: 100,
            last_modified: 1609459200,
        };

        let change = S3::determine_change(&obj, Some(prev));

        // No change should be detected when etag is the same
        assert!(change.is_none());
    }
}
