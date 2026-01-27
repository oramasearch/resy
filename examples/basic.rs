use resy::Change;
use resy::s3::S3;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build S3 client with builder pattern
    let s3 = S3::builder()
        .bucket("my-bucket")
        .region("us-east-1")
        .credentials("AKIA...", "secret...")
        // Optional: configure endpoint for LocalStack or MinIO
        .endpoint("http://localhost:4566")
        // Optional: configure page size for pagination
        .batch_size(500)
        .build()
        .await?;

    // Stream changes - db_path is auto-generated as "{bucket}.db" if not specified
    let mut stream = s3.stream_changes("resy.db").await.unwrap();

    while let Some(result) = stream.next().await {
        match result {
            Ok(change) => match change {
                Change::Added(obj) => {
                    println!("Added: {} ({} bytes)", obj.key, obj.size);
                }
                Change::Modified { old, new } => {
                    println!("Modified: {} ({} -> {} bytes)", new.key, old.size, new.size);
                }
                Change::Deleted(obj) => {
                    println!("Deleted: {} ({} bytes)", obj.key, obj.size);
                }
            },
            Err(e) => {
                eprintln!("Error processing change: {}", e);
                break;
            }
        }
    }

    Ok(())
}
