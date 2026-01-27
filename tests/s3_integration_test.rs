use aws_config::SdkConfig;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    Client,
    config::{self, BehaviorVersion, Region},
};
use testcontainers::{ContainerAsync, GenericImage, core::WaitFor, runners::AsyncRunner};
use testcontainers::{ImageExt, core::ContainerPort};
use tokio_stream::StreamExt;

async fn setup_localstack_s3() -> (ContainerAsync<GenericImage>, Client) {
    let localstack_port = 4566;
    let container = GenericImage::new("localstack/localstack", "s3-latest")
        .with_exposed_port(ContainerPort::Tcp(localstack_port))
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .with_env_var("SERVICES", "s3")
        .start()
        .await
        .unwrap();

    let host = container.get_host().await.unwrap();
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(localstack_port))
        .await
        .unwrap();
    let endpoint_url = format!("http://{}:{}", host, host_port);

    let credentials = Credentials::new("test", "test", None, None, "test");
    let config = SdkConfig::builder()
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .endpoint_url(endpoint_url)
        .region(Region::new("us-east-1"))
        .behavior_version(BehaviorVersion::latest())
        .build();

    let s3_config = config::Builder::from(&config)
        .force_path_style(true)
        .build();

    let client = Client::from_conf(s3_config);

    (container, client)
}

#[tokio::test]
async fn test_stream_changes() {
    let (_container, s3_client) = setup_localstack_s3().await;

    let bucket_name = "my-test-bucket";

    s3_client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .expect("Failed to create S3 bucket");

    let list_buckets_output = s3_client
        .list_buckets()
        .send()
        .await
        .expect("Failed to list S3 buckets");

    let buckets = list_buckets_output.buckets();
    let found = buckets.iter().any(|b| b.name() == Some(bucket_name));
    assert!(found, "Bucket '{}' was not found in the list.", bucket_name);

    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = db_file.path();

    let s3 = resy::s3::S3::from_client(s3_client.clone(), bucket_name.to_string(), None);

    // 1. Initial check: No changes
    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut changes = Vec::new();
    while let Some(result) = stream.next().await {
        changes.push(result.unwrap());
    }
    assert_eq!(changes.len(), 0);

    // 2. Add a file
    let key = "test-file.txt";
    let content = "Hello, Resy!";
    s3_client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            content.as_bytes().to_vec(),
        ))
        .send()
        .await
        .unwrap();

    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut changes = Vec::new();
    while let Some(result) = stream.next().await {
        changes.push(result.unwrap());
    }

    assert_eq!(changes.len(), 1);
    match &changes[0] {
        resy::Change::Added(obj) => {
            assert_eq!(obj.key, key);
            assert_eq!(obj.size, content.len() as i64);
        }
        _ => panic!("Expected Added change"),
    }

    // 3. Modify the file
    let updated_content = "Hello, Resy! Updated.";
    s3_client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            updated_content.as_bytes().to_vec(),
        ))
        .send()
        .await
        .unwrap();

    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut changes = Vec::new();
    while let Some(result) = stream.next().await {
        changes.push(result.unwrap());
    }

    assert_eq!(changes.len(), 1);
    match &changes[0] {
        resy::Change::Modified { old, new } => {
            assert_eq!(new.key, key);
            assert_eq!(old.size, content.len() as i64);
            assert_eq!(new.size, updated_content.len() as i64);
        }
        _ => panic!("Expected Modified change"),
    }

    // 4. Delete the file
    s3_client
        .delete_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut changes = Vec::new();
    while let Some(result) = stream.next().await {
        changes.push(result.unwrap());
    }

    assert_eq!(changes.len(), 1);
    match &changes[0] {
        resy::Change::Deleted(obj) => {
            assert_eq!(obj.key, key);
            assert_eq!(obj.size, updated_content.len() as i64);
        }
        _ => panic!("Expected Deleted change"),
    }
}

#[tokio::test]
async fn test_stream_stops_at_first_error() {
    let s3_client = Client::from_conf(
        config::Builder::new()
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "test", "test", None, None, "test",
            )))
            .endpoint_url("http://invalid-endpoint:9999")
            .region(Region::new("us-east-1"))
            .behavior_version(BehaviorVersion::latest())
            .force_path_style(true)
            .build(),
    );

    let s3 = resy::s3::S3::from_client(s3_client, "test-bucket".to_string(), None);
    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = db_file.path();

    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut error_occurred = false;

    while let Some(result) = stream.next().await {
        if result.is_err() {
            // should quit the stream
            error_occurred = true;
        }
    }

    assert!(
        error_occurred,
        "Should receive an error from invalid endpoint"
    );

    let remaining: Vec<_> = stream.collect().await;
    assert_eq!(
        remaining.len(),
        0,
        "Stream should stop after first error, no more items should be yielded"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_db_access() {
    let (_container, s3_client) = setup_localstack_s3().await;

    let bucket_name = "my-test-bucket";

    s3_client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .expect("Failed to create S3 bucket");

    for i in 1..10 {
        s3_client
            .put_object()
            .bucket(bucket_name)
            .key(format!("test-{i}"))
            .body(aws_sdk_s3::primitives::ByteStream::from(
                format!("hello-{i}").as_bytes().to_vec(),
            ))
            .send()
            .await
            .unwrap();
    }

    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = db_file.path();

    let s3 = resy::s3::S3::from_client(s3_client.clone(), bucket_name.to_string(), Some(1));

    let stream1 = s3.stream_changes(db_path).await.unwrap();
    let stream2 = s3.stream_changes(db_path).await;
    assert!(stream2.is_err());
    assert!(
        stream2
            .unwrap_err()
            .to_string()
            .contains("error returned from database: (code: 5) database is locked")
    );

    drop(stream1);
}

#[tokio::test]
async fn test_per_batch_commit_crash_recovery() {
    let (_container, s3_client) = setup_localstack_s3().await;

    let bucket_name = "batch-test-bucket";

    s3_client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .expect("Failed to create S3 bucket");

    for i in 0..25 {
        s3_client
            .put_object()
            .bucket(bucket_name)
            .key(format!("file-{:03}.txt", i))
            .body(aws_sdk_s3::primitives::ByteStream::from(
                format!("content-{}", i).as_bytes().to_vec(),
            ))
            .send()
            .await
            .unwrap();
    }

    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = db_file.path();

    let s3 = resy::s3::S3::from_client(s3_client.clone(), bucket_name.to_string(), Some(8));

    // First sync: Simulate a "crash" by only consuming first 10 changes
    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut first_sync_changes = Vec::new();
    for _ in 0..10 {
        if let Some(result) = stream.next().await {
            first_sync_changes.push(result.unwrap());
        }
    }
    // Drop the stream to simulate crash (remaining changes won't be consumed)
    drop(stream);

    assert_eq!(
        first_sync_changes.len(),
        10,
        "Should have processed 10 changes before 'crash'"
    );

    // Verify all 10 changes are Added
    for change in &first_sync_changes {
        match change {
            resy::Change::Added(_) => {}
            _ => panic!("Expected all changes to be Added in first sync"),
        }
    }

    // Second sync: Restart after "crash"
    // we should get 15 (remaining objects) + 2 (previous run - batch size)
    // we prefer to reprocess objects instead of silently ignoring them.
    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut second_sync_changes = Vec::new();
    while let Some(result) = stream.next().await {
        second_sync_changes.push(result.unwrap());
    }
    assert_eq!(second_sync_changes.len(), 17);

    for change in &second_sync_changes {
        match change {
            resy::Change::Added(_) => {}
            _ => panic!("Expected all changes to be Added in second sync"),
        }
    }

    // Third sync: Should report no changes since everything is synced
    let mut stream = s3.stream_changes(db_path).await.unwrap();
    let mut third_sync_changes = Vec::new();
    while let Some(result) = stream.next().await {
        third_sync_changes.push(result.unwrap());
    }
    assert_eq!(third_sync_changes.len(), 0,);
}
