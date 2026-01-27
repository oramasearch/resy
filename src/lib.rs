#![doc = include_str!("../README.md")]

pub mod error;
pub mod s3;

pub use error::ResyError;
pub use s3::{Change, S3Object};

use std::future::Future;
use std::path::PathBuf;
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
        db_path: PathBuf,
    ) -> impl Future<Output = Result<ChangeStream<Self::Change>, ResyError>> + Send;
}
