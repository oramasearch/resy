use async_stream::stream;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::SharedCredentialsProvider;
use chrono::{DateTime, Utc};
use futures_core::Stream;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::path::Path;
use std::pin::Pin;
use zeroize::{Zeroize, ZeroizeOnDrop};

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
    size: u64,
    last_modified: i64,
}

/// Represents a change detected in an S3 bucket
#[derive(Debug, PartialEq)]
pub enum Change {
    /// A new object was added
    Added(S3Object),
    /// An existing object was modified
    Modified {
        /// Previous state of the object
        old: S3Object,
        /// Current state of the object
        new: S3Object,
    },
    /// An object was deleted
    Deleted(S3Object),
}

pub type ChangeStream<'a> = Pin<
    Box<dyn Stream<Item = Result<Change, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
>;

/// Error type for S3Builder
#[derive(Debug)]
pub enum S3BuilderError {
    /// A required field was not set
    MissingField(&'static str),
    /// Database error during initialization
    SqlxError(sqlx::Error),
}

impl std::fmt::Display for S3BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3BuilderError::MissingField(field) => write!(f, "Missing required field: {}", field),
            S3BuilderError::SqlxError(e) => write!(f, "Database error: {}", e),
        }
    }
}

impl std::error::Error for S3BuilderError {}

impl From<sqlx::Error> for S3BuilderError {
    fn from(e: sqlx::Error) -> Self {
        S3BuilderError::SqlxError(e)
    }
}

/// Builder for configuring an S3 client
///
/// # Example
/// ```no_run
/// # use resy::s3::S3;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let s3 = S3::builder()
///     .bucket("my-bucket")
///     .region("us-west-2")
///     .credentials("access_key", "secret_key")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct S3Builder {
    bucket: Option<String>,
    #[zeroize(skip)]
    region: Option<String>,
    access_key_id: Option<SecretString>,
    secret_access_key: Option<SecretString>,
    #[zeroize(skip)]
    endpoint_url: Option<String>,
    #[zeroize(skip)]
    page_size: Option<i32>,
    #[zeroize(skip)]
    db_path: Option<std::path::PathBuf>,
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
            access_key_id: None,
            secret_access_key: None,
            endpoint_url: None,
            page_size: Some(1000), // Default page size
            db_path: None,
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
        self.access_key_id = Some(SecretString::new(access_key_id.into().into()));
        self.secret_access_key = Some(SecretString::new(secret_access_key.into().into()));
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint.into());
        self
    }

    pub fn page_size(mut self, size: i32) -> Self {
        self.page_size = Some(size);
        self
    }

    pub fn db_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.db_path = Some(path.into());
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
        let access_key_id = self
            .access_key_id
            .take()
            .ok_or(S3BuilderError::MissingField("credentials"))?;
        let secret_access_key = self
            .secret_access_key
            .take()
            .ok_or(S3BuilderError::MissingField("credentials"))?;

        let credentials = Credentials::new(
            access_key_id.expose_secret(),
            secret_access_key.expose_secret(),
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
            page_size: self.page_size.unwrap_or(1000),
            db_path: self.db_path.take(),
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
/// let mut changes = s3.stream_changes(None);
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
    page_size: i32,
    db_path: Option<std::path::PathBuf>,
}

impl S3 {
    /// Create a new S3Builder to configure and build an S3 client
    pub fn builder() -> S3Builder {
        S3Builder::new()
    }

    /// Create an S3 instance from an existing AWS SDK S3 client
    pub fn from_client(client: Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
            page_size: 1000,
            db_path: None,
        }
    }

    pub async fn create_state_db(db_path: &Path) -> Result<SqlitePool, sqlx::Error> {
        let db_url = format!("sqlite:{}", db_path.display());
        let pool = SqlitePool::connect(&db_url).await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS object_state (
            key TEXT PRIMARY KEY,
            etag TEXT NOT NULL,
            size INTEGER NOT NULL,
            last_modified INTEGER NOT NULL
        )",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
        )
        .execute(&pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_etag ON object_state(etag)")
            .execute(&pool)
            .await?;

        Ok(pool)
    }

    /// Stream changes using the configured or auto-generated database path
    pub fn stream_changes(&self, db_path: Option<&Path>) -> ChangeStream<'_> {
        let actual_db_path = db_path
            .map(|p| p.to_path_buf())
            .or_else(|| self.db_path.clone())
            .unwrap_or_else(|| {
                let bucket_name = self.bucket.replace(['/', '\\', ':'], "_");
                std::path::PathBuf::from(format!("{}.db", bucket_name))
            });
        self.stream_diff_and_update(&actual_db_path)
    }

    pub fn stream_diff_and_update(&self, db_path: &Path) -> ChangeStream<'_> {
        let db_path = db_path.to_path_buf();
        let bucket = self.bucket.clone();
        let client = self.client.clone();
        let page_size = self.page_size;

        Box::pin(stream! {
            let pool = match Self::create_state_db(&db_path).await {
                Ok(p) => p,
                Err(e) => {
                    yield Err(e.into());
                    return;
                }
            };

            // Start transaction
            let mut tx = match pool.begin().await {
                Ok(t) => t,
                Err(e) => {
                    yield Err(e.into());
                    return;
                }
            };

            sqlx::query("ALTER TABLE object_state ADD COLUMN temp_seen INTEGER DEFAULT 0")
                .execute(&mut *tx)
                .await
                .ok();

            // required to support SQLite versions < 3.35.0 that do not support DROP COLUMN
            if let Err(e) = sqlx::query("UPDATE object_state SET temp_seen = 0")
                .execute(&mut *tx)
                .await
            {
                yield Err(e.into());
                return;
            }

            let mut continuation_token: Option<String> = None;
            loop {
                let mut request = client.list_objects_v2().bucket(&bucket).max_keys(page_size);
                if let Some(token) = continuation_token.take() {
                    request = request.continuation_token(token);
                }

                let response = match request.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        yield Err(e.into());
                        return;
                    }
                };

                for obj in response.contents() {
                    let Some(current_obj) = Self::parse_aws_object(obj) else { continue };

                    let previous_state = match sqlx::query(
                        "SELECT etag, size, last_modified FROM object_state WHERE key = ?1"
                    )
                    .bind(&current_obj.key)
                    .fetch_optional(&mut *tx)
                    .await
                    {
                        Ok(Some(row)) => Some(CompactS3Object {
                            etag: row.get(0),
                            size: row.get(1),
                            last_modified: row.get(2),
                        }),
                        Ok(None) => None,
                        Err(e) => {
                            yield Err(e.into());
                            return;
                        }
                    };

                    let change = Self::determine_change(&current_obj, previous_state.clone());

                    if previous_state.is_none() {
                        if let Err(e) = sqlx::query(
                            "INSERT OR REPLACE INTO object_state (key, etag, size, last_modified, temp_seen) \
                             VALUES (?1, ?2, ?3, ?4, 1)"
                        )
                        .bind(&current_obj.key)
                        .bind(&current_obj.etag)
                        .bind(current_obj.size)
                        .bind(current_obj.last_modified.timestamp())
                        .execute(&mut *tx)
                        .await
                        {
                            yield Err(e.into());
                            return;
                        }
                    } else {
                        if let Err(e) = sqlx::query("UPDATE object_state SET temp_seen = 1 WHERE key = ?1")
                            .bind(&current_obj.key)
                            .execute(&mut *tx)
                            .await
                        {
                            yield Err(e.into());
                            return;
                        }

                        if change.is_some() {
                            if let Err(e) = sqlx::query(
                                "INSERT OR REPLACE INTO object_state (key, etag, size, last_modified, temp_seen) \
                                 VALUES (?1, ?2, ?3, ?4, 1)"
                            )
                            .bind(&current_obj.key)
                            .bind(&current_obj.etag)
                            .bind(current_obj.size)
                            .bind(current_obj.last_modified.timestamp())
                            .execute(&mut *tx)
                            .await
                            {
                                yield Err(e.into());
                                return;
                            }
                        }
                    }

                    if let Some(change) = change {
                        yield Ok(change);
                    }
                }

                if response.is_truncated().unwrap_or(false) {
                    continuation_token = response.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }

            let deleted_rows: Vec<(String, CompactS3Object)> = match sqlx::query(
                "SELECT key, etag, size, last_modified FROM object_state WHERE temp_seen = 0"
            )
            .fetch_all(&mut *tx)
            .await
            {
                Ok(rows) => rows.into_iter().map(|row| {
                    (
                        row.get::<String, _>(0),
                        CompactS3Object {
                            etag: row.get(1),
                            size: row.get(2),
                            last_modified: row.get(3),
                        },
                    )
                }).collect(),
                Err(e) => {
                    yield Err(e.into());
                    return;
                }
            };

            for (key, prev_obj) in deleted_rows {
                yield Ok(Change::Deleted(Self::compact_to_s3_object(&key, &prev_obj)));

                if let Err(e) = sqlx::query("DELETE FROM object_state WHERE key = ?1")
                    .bind(&key)
                    .execute(&mut *tx)
                    .await
                {
                    yield Err(e.into());
                    return;
                }
            }

            // Clean up the temporary column (for next run)
            // SQLite doesn't support DROP COLUMN before version 3.35.0
            // so let's make sure we use version >= 3.35.0 in production
            sqlx::query("ALTER TABLE object_state DROP COLUMN IF EXISTS temp_seen")
                .execute(&mut *tx)
                .await
                .ok();

            if let Err(e) = sqlx::query(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_updated', ?1)"
            )
            .bind(Utc::now().timestamp())
            .execute(&mut *tx)
            .await
            {
                yield Err(e.into());
                return;
            }

            if let Err(e) = tx.commit().await {
                yield Err(e.into());
                return;
            }
        })
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
            size: compact.size as i64,
            last_modified: DateTime::from_timestamp(compact.last_modified, 0)
                .unwrap_or_default()
                .with_timezone(&Utc),
        }
    }
}

// Implement DataSource trait for S3
impl crate::DataSource for S3 {
    type Change = Change;

    fn stream_changes(
        &self,
        db_path: Option<&Path>,
    ) -> impl Stream<Item = Result<Self::Change, Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        S3::stream_changes(self, db_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::NamedTempFile;

    fn create_test_s3_object(key: &str, etag: &str, size: i64, timestamp: i64) -> S3Object {
        S3Object {
            key: key.to_string(),
            etag: etag.to_string(),
            size,
            last_modified: Utc.timestamp_opt(timestamp, 0).unwrap(),
        }
    }

    #[test]
    fn test_s3_object_creation() {
        let obj = create_test_s3_object("test/file.txt", "etag123", 1024, 1609459200);

        assert_eq!(obj.key, "test/file.txt");
        assert_eq!(obj.etag, "etag123");
        assert_eq!(obj.size, 1024);
        assert_eq!(obj.last_modified, Utc.timestamp_opt(1609459200, 0).unwrap());
    }

    #[test]
    fn test_change_enum_variants() {
        let obj1 = create_test_s3_object("test1", "etag1", 100, 1609459200);
        let obj2 = create_test_s3_object("test2", "etag2", 200, 1609459300);
        let obj3 = create_test_s3_object("test3", "etag3", 300, 1609459400);

        let added = Change::Added(obj1.clone());
        let modified = Change::Modified {
            old: obj1.clone(),
            new: obj2.clone(),
        };
        let deleted = Change::Deleted(obj3.clone());

        match added {
            Change::Added(ref obj) => assert_eq!(obj.key, "test1"),
            _ => panic!("Expected Added variant"),
        }

        match modified {
            Change::Modified { ref old, ref new } => {
                assert_eq!(old.key, "test1");
                assert_eq!(new.key, "test2");
            }
            _ => panic!("Expected Modified variant"),
        }

        match deleted {
            Change::Deleted(ref obj) => assert_eq!(obj.key, "test3"),
            _ => panic!("Expected Deleted variant"),
        }
    }

    #[tokio::test]
    async fn test_create_state_db() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let pool = S3::create_state_db(db_path).await.unwrap();

        let tables: Vec<String> = sqlx::query("SELECT name FROM sqlite_master WHERE type='table'")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect();

        assert!(tables.contains(&"object_state".to_string()));
        assert!(tables.contains(&"metadata".to_string()));

        let index_exists =
            sqlx::query("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_etag'")
                .fetch_optional(&pool)
                .await
                .unwrap()
                .is_some();
        assert!(index_exists);
    }

    #[test]
    fn test_compact_to_s3_object() {
        let compact = CompactS3Object {
            etag: "etag123".to_string(),
            size: 1024,
            last_modified: 1609459200,
        };

        let s3_obj = S3::compact_to_s3_object("test/file.txt", &compact);

        assert_eq!(s3_obj.key, "test/file.txt");
        assert_eq!(s3_obj.etag, "etag123");
        assert_eq!(s3_obj.size, 1024);
        assert_eq!(
            s3_obj.last_modified,
            Utc.timestamp_opt(1609459200, 0).unwrap()
        );
    }

    #[tokio::test]
    async fn test_database_operations() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let pool = S3::create_state_db(db_path).await.unwrap();

        sqlx::query(
            "INSERT INTO object_state (key, etag, size, last_modified) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind("test/file.txt")
        .bind("etag123")
        .bind(1024_i64)
        .bind(1609459200_i64)
        .execute(&pool)
        .await
        .unwrap();

        let row =
            sqlx::query("SELECT key, etag, size, last_modified FROM object_state WHERE key = ?1")
                .bind("test/file.txt")
                .fetch_one(&pool)
                .await
                .unwrap();

        let key: String = row.get(0);
        let etag: String = row.get(1);
        let size: i64 = row.get(2);
        let last_modified: i64 = row.get(3);

        assert_eq!(key, "test/file.txt");
        assert_eq!(etag, "etag123");
        assert_eq!(size, 1024);
        assert_eq!(last_modified, 1609459200);
    }

    #[tokio::test]
    async fn test_database_with_temp_seen_column() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let pool = S3::create_state_db(db_path).await.unwrap();

        sqlx::query(
            "INSERT INTO object_state (key, etag, size, last_modified) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind("test/file.txt")
        .bind("etag123")
        .bind(1024_i64)
        .bind(1609459200_i64)
        .execute(&pool)
        .await
        .unwrap();

        let mut tx = pool.begin().await.unwrap();

        sqlx::query("ALTER TABLE object_state ADD COLUMN temp_seen INTEGER DEFAULT 0")
            .execute(&mut *tx)
            .await
            .unwrap();

        sqlx::query("UPDATE object_state SET temp_seen = 1 WHERE key = ?1")
            .bind("test/file.txt")
            .execute(&mut *tx)
            .await
            .unwrap();

        let row = sqlx::query("SELECT temp_seen FROM object_state WHERE key = ?1")
            .bind("test/file.txt")
            .fetch_one(&mut *tx)
            .await
            .unwrap();

        let temp_seen: i32 = row.get(0);
        assert_eq!(temp_seen, 1);

        tx.commit().await.unwrap();
    }

    #[test]
    fn test_s3_object_equality() {
        let obj1 = create_test_s3_object("test", "etag1", 100, 1609459200);
        let obj2 = create_test_s3_object("test", "etag1", 100, 1609459200);
        let obj3 = create_test_s3_object("test", "etag2", 100, 1609459200);

        assert_eq!(obj1, obj2);
        assert_ne!(obj1, obj3);
    }

    #[test]
    fn test_change_equality() {
        let obj1 = create_test_s3_object("test1", "etag1", 100, 1609459200);
        let obj2 = create_test_s3_object("test2", "etag2", 200, 1609459300);

        let change1 = Change::Added(obj1.clone());
        let change2 = Change::Added(obj1.clone());
        let change3 = Change::Added(obj2.clone());

        assert_eq!(change1, change2);
        assert_ne!(change1, change3);
    }

    #[tokio::test]
    async fn test_builder_success() {
        let s3 = S3::builder()
            .bucket("test-bucket")
            .region("us-west-2")
            .credentials("test-key", "test-secret")
            .build()
            .await;

        assert!(s3.is_ok());
        let s3 = s3.unwrap();
        assert_eq!(s3.bucket, "test-bucket");
        assert_eq!(s3.page_size, 1000); // default
    }

    #[tokio::test]
    async fn test_builder_with_optional_fields() {
        let s3 = S3::builder()
            .bucket("test-bucket")
            .region("us-west-2")
            .credentials("test-key", "test-secret")
            .endpoint("http://localhost:4566")
            .page_size(500)
            .build()
            .await;

        assert!(s3.is_ok());
        let s3 = s3.unwrap();
        assert_eq!(s3.page_size, 500);
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
}
