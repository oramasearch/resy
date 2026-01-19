pub mod s3;

// Re-export common types at the root for easy access
pub use s3::{Change, S3Object};

use futures_core::Stream;
use std::path::Path;

/// Trait for data sources that can stream changes
pub trait DataSource {
    type Change;

    /// Stream changes from the data source, using the specified database path for state tracking
    fn stream_changes(
        &self,
        db_path: Option<&Path>,
    ) -> impl Stream<Item = Result<Self::Change, Box<dyn std::error::Error + Send + Sync>>> + Send;
}
