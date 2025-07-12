use mockito::Server;
use sailhouse::{SailhouseClient, SubscriberOptions};
use serde_json::json;

fn create_test_client(server: &Server) -> SailhouseClient {
    let client = reqwest::Client::new();
    SailhouseClient::with_base_url(client, "test-token".to_string(), server.url())
}

#[tokio::test(flavor = "current_thread")]
async fn test_pull_event() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);

    let response_data = json!({
        "events": [{
            "id": "event-1",
            "data": {
                "message": "test message"
            },
            "metadata": {
                "source": "test"
            }
        }],
        "offset": 0,
        "limit": 1
    });

    let mock = server
        .mock(
            "GET",
            "/topics/test-topic/subscriptions/test-sub/events?limit=1&offset=0",
        )
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .with_body(response_data.to_string())
        .create();

    let result = client.pull("test-topic", "test-sub").await.unwrap();

    assert!(result.is_some());
    let event = result.unwrap();
    assert_eq!(event.id, "event-1");
    assert_eq!(event.topic, "test-topic");
    assert_eq!(event.subscription, "test-sub");
    assert!(event.metadata.is_some());
    
    let metadata = event.metadata.unwrap();
    assert_eq!(metadata.get("source").unwrap(), "test");
    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_pull_no_events() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);

    let response_data = json!({
        "events": [],
        "offset": 0,
        "limit": 1
    });

    let mock = server
        .mock(
            "GET",
            "/topics/test-topic/subscriptions/test-sub/events?limit=1&offset=0",
        )
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .with_body(response_data.to_string())
        .create();

    let result = client.pull("test-topic", "test-sub").await.unwrap();

    assert!(result.is_none());
    mock.assert();
}

#[test]
fn test_subscriber_creation() {
    let mut server = mockito::Server::new();
    let client = create_test_client(&server);

    let subscriber = client.subscriber(None);
    // Test that subscriber was created successfully
    // Note: We can't easily test the full subscriber functionality without 
    // significant refactoring due to the async nature and complex ownership
}

#[test]
fn test_subscriber_creation_with_options() {
    let mut server = mockito::Server::new();
    let client = create_test_client(&server);

    let options = SubscriberOptions {
        per_subscription_processors: 5,
    };

    let subscriber = client.subscriber(Some(options));
    // Test that subscriber was created with custom options
}