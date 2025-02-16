use mockito::Server;
use sailhouse::{Event, GetOption, SailhouseClient};
use serde::Deserialize;
use serde_json::json;

fn create_test_client(server: &Server) -> SailhouseClient {
    let client = reqwest::Client::new();
    SailhouseClient::with_base_url(client, "test-token".to_string(), server.url())
}

#[tokio::test(flavor = "current_thread")]
async fn test_publish() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);

    let mock = server
        .mock("POST", "/topics/test-topic/events")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(201)
        .create();

    let data = json!({
        "message": "test message"
    });

    let result = client.publish("test-topic", data).await;
    assert!(result.is_ok());
    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_get_events() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);

    let response_data = json!({
        "events": [{
            "id": "event-1",
            "data": {
                "message": "test message"
            }
        }],
        "offset": 0,
        "limit": 10
    });

    let mock = server
        .mock(
            "GET",
            "/topics/test-topic/subscriptions/test-sub/events?limit=10&offset=0",
        )
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .with_body(response_data.to_string())
        .create();

    let opts = GetOption {
        limit: Some(10),
        offset: Some(0),
    };

    let result = client
        .get_events("test-topic", "test-sub", opts)
        .await
        .unwrap();

    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].id, "event-1");
    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_acknowledge_message() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);

    let mock = server
        .mock(
            "POST",
            "/topics/test-topic/subscriptions/test-sub/events/event-1",
        )
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .create();

    let result = client
        .acknowledge_message("test-topic", "test-sub", "event-1")
        .await;
    assert!(result.is_ok());
    mock.assert();
}

#[test]
fn test_event_deserialization() {
    let event = Event {
        id: "event-1".to_string(),
        data: json!({
            "message": "test message",
            "count": 42
        }),
        topic: "test-topic".to_string(),
        subscription: "test-sub".to_string(),
        client: None,
    };

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestData {
        message: String,
        count: i32,
    }

    let deserialized: TestData = event.deserialize().unwrap();
    assert_eq!(
        deserialized,
        TestData {
            message: "test message".to_string(),
            count: 42
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_event_ack() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);

    let mock = server
        .mock(
            "POST",
            "/topics/test-topic/subscriptions/test-sub/events/event-1",
        )
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .create();

    let event = Event {
        id: "event-1".to_string(),
        data: json!({}),
        topic: "test-topic".to_string(),
        subscription: "test-sub".to_string(),
        client: Some(client),
    };

    let result = event.ack().await;
    assert!(result.is_ok());
    mock.assert();
}
