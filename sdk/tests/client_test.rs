use mockito::Server;
use sailhouse::{Event, GetOption, SailhouseClient, WaitEvent, WaitOptions};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;

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
        .with_body(r#"{"id":"test-event-id"}"#)
        .create();

    let data = json!({
        "message": "test message"
    });

    let result = client.publish("test-topic", data).send().await;
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

#[tokio::test(flavor = "current_thread")]
async fn test_wait() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);

    // Mock wait group creation
    let wait_group_mock = server
        .mock("POST", "/waitgroups/instances")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .with_body(r#"{"wait_group_instance_id":"test-wait-group-id"}"#)
        .create();

    // Mock event publishing - one event using the main topic
    let publish_mock = server
        .mock("POST", "/topics/test-topic/events")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(201)
        .with_body(r#"{"id":"event-id"}"#)
        .expect(2) // Expect two calls
        .create();

    // Mock wait group completion
    let complete_mock = server
        .mock("PUT", "/waitgroups/instances/test-wait-group-id/events")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .create();

    let events = vec![
        WaitEvent {
            topic: "test-topic".to_string(), // Match the topic from client.wait()
            body: json!({"message": "event 1"}),
            metadata: None,
            send_at: None,
        },
        WaitEvent {
            topic: "test-topic".to_string(), // Match the topic from client.wait()
            body: json!({"message": "event 2"}),
            metadata: Some(HashMap::from([("source".to_string(), "test".to_string())])),
            send_at: None,
        },
    ];

    let options = WaitOptions {
        ttl: Some("5m".to_string()),
    };

    let result = client.wait("test-topic", events, Some(options)).await;
    assert!(result.is_ok());

    wait_group_mock.assert();
    publish_mock.assert();
    complete_mock.assert();
}
