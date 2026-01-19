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

#[derive(Clone, Debug, PartialEq)]
pub struct S3Object {
    pub key: String,
    pub etag: String,
    pub size: i64,
    pub last_modified: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CompactS3Object {
    etag: String,
    size: u64,
    last_modified: i64,
}

#[derive(Debug, PartialEq)]
pub enum Change {
    Added(S3Object),
    Modified { old: S3Object, new: S3Object },
    Deleted(S3Object),
}

pub type ChangeStream<'a> = Pin<
    Box<dyn Stream<Item = Result<Change, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
>;

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct S3Conf {
    #[zeroize(skip)]
    pub bucket: String,
    #[zeroize(skip)]
    pub region: String,
    pub access_key_id: SecretString,
    pub secret_access_key: SecretString,
    #[zeroize(skip)]
    pub endpoint_url: Option<String>,
}

impl S3Conf {
    pub fn new(
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        region: String,
    ) -> Self {
        S3Conf {
            bucket,
            access_key_id: SecretString::new(access_key_id.into()),
            secret_access_key: SecretString::new(secret_access_key.into()),
            region,
            endpoint_url: None,
        }
    }

    pub fn new_with_endpoint(
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        region: String,
        endpoint_url: String,
    ) -> Self {
        S3Conf {
            bucket,
            access_key_id: SecretString::new(access_key_id.into()),
            secret_access_key: SecretString::new(secret_access_key.into()),
            region,
            endpoint_url: Some(endpoint_url),
        }
    }

    fn get_access_key_id(&self) -> &str {
        self.access_key_id.expose_secret()
    }

    fn get_secret_access_key(&self) -> &str {
        self.secret_access_key.expose_secret()
    }
}

pub struct S3 {
    client: Client,
    bucket: String,
}

impl S3 {
    pub async fn new(conf: &S3Conf) -> Self {
        Self::create_client(conf).await
    }

    pub fn from_client(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    async fn create_client(conf: &S3Conf) -> Self {
        let credentials = Credentials::new(
            conf.get_access_key_id(),
            conf.get_secret_access_key(),
            None,
            None,
            "resy",
        );

        let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(Region::new(conf.region.clone()))
            .behavior_version(BehaviorVersion::latest());

        // ideally we should only use endpoint_url for local testing.
        // We may want to add a flag to disable it in prod.
        if let Some(endpoint_url) = &conf.endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_config = s3_config_builder.build();
        let s3_client = Client::from_conf(s3_config);

        Self {
            client: s3_client,
            bucket: conf.bucket.clone(),
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

    pub fn stream_diff_and_update(&self, db_path: &Path) -> ChangeStream<'_> {
        let db_path = db_path.to_path_buf();
        let bucket = self.bucket.clone();
        let client = self.client.clone();

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
                let mut request = client.list_objects_v2().bucket(&bucket).max_keys(1000);
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

    fn create_test_s3_conf() -> S3Conf {
        S3Conf::new(
            "test-bucket".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            "us-west-2".to_string(),
        )
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
    fn test_s3_struct_creation() {
        let s3 = create_test_s3_conf();

        assert_eq!(s3.bucket, "test-bucket");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.get_access_key_id(), "test-key");
        assert_eq!(s3.get_secret_access_key(), "test-secret");
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

    #[test]
    fn test_s3_with_endpoint() {
        let s3 = S3Conf::new_with_endpoint(
            "test-bucket".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            "us-west-2".to_string(),
            "http://localhost:4566".to_string(),
        );

        assert_eq!(s3.bucket, "test-bucket");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.endpoint_url, Some("http://localhost:4566".to_string()));
        assert_eq!(s3.get_access_key_id(), "test-key");
        assert_eq!(s3.get_secret_access_key(), "test-secret");
    }

    #[test]
    fn test_s3_without_endpoint() {
        let s3 = S3Conf::new(
            "test-bucket".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            "us-west-2".to_string(),
        );

        assert_eq!(s3.bucket, "test-bucket");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.endpoint_url, None);
        assert_eq!(s3.get_access_key_id(), "test-key");
        assert_eq!(s3.get_secret_access_key(), "test-secret");
    }
}
