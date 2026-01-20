//! # Resy - Remote Sync Change Detection Library
//!
//! Resy monitors remote data sources (currently S3) and streams detected changes
//! to consuming applications.
//!
//! ## Quick Start
//!
//! ```no_run
//! use resy::s3::S3;
//! use resy::Change;
//! use tokio_stream::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let s3 = S3::builder()
//!         .bucket("my-bucket")
//!         .region("us-east-1")
//!         .credentials("key", "secret")
//!         .build()
//!         .await?;
//!
//!     let mut stream = s3.stream_changes(None).await.unwrap();
//!     while let Some(result) = stream.next().await {
//!         match result {
//!             Ok(Change::Added(obj)) => println!("Added: {}", obj.key),
//!             Ok(Change::Modified { new, .. }) => println!("Modified: {}", new.key),
//!             Ok(Change::Deleted(obj)) => println!("Deleted: {}", obj.key),
//!             Err(e) => eprintln!("Error: {}", e),
//!         }
//!     }
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod s3;

pub use error::ResyError;
pub use s3::{Change, S3Object};

use std::future::Future;
use std::path::Path;
use tokio_stream::wrappers::ReceiverStream;

pub type ChangeStream<T> = ReceiverStream<Result<T, ResyError>>;

/// Trait for data sources that can stream changes
///
/// This trait enables generic code to work with different data sources
/// (S3, Snowflake, etc.) in a uniform way.
pub trait DataSource {
    type Change;

    /// Stream changes from the data source, using the specified database path for state tracking.
    ///
    /// If `db_path` is `None`, a default path will be generated based on the data source configuration.
    fn stream_changes(
        &self,
        db_path: Option<&Path>,
    ) -> impl Future<Output = Result<ChangeStream<Self::Change>, ResyError>> + Send;
}
