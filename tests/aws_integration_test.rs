use aws_config::SdkConfig;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    Client,
    config::{self, BehaviorVersion, Region},
};
use testcontainers::{GenericImage, core::WaitFor, runners::AsyncRunner};
use testcontainers::{ImageExt, core::ContainerPort};
use tokio_stream::StreamExt;

#[tokio::test]
async fn test_stream_diff_and_update() {
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

    let s3_client = Client::from_conf(s3_config);

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

    let s3 = resy::s3::S3::from_client(s3_client.clone(), bucket_name.to_string());

    // 1. Initial check: No changes
    let mut stream = s3.stream_diff_and_update(db_path);
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

    let mut stream = s3.stream_diff_and_update(db_path);
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

    let mut stream = s3.stream_diff_and_update(db_path);
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

    let mut stream = s3.stream_diff_and_update(db_path);
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

    let s3 = resy::s3::S3::from_client(s3_client, "test-bucket".to_string());
    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = db_file.path();

    let mut stream = s3.stream_diff_and_update(db_path);
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
