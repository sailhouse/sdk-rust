use mockito::Server;
use sailhouse::{
    AdminClient, Filter, FilterCondition, ComplexFilter, PushSubscriptionOptions, SailhouseClient,
};
use serde_json::json;

fn create_test_client(server: &Server) -> SailhouseClient {
    let client = reqwest::Client::new();
    SailhouseClient::with_base_url(client, "test-token".to_string(), server.url())
}

#[tokio::test(flavor = "current_thread")]
async fn test_register_push_subscription_simple() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);
    let admin = AdminClient::new(client);

    let mock = server
        .mock("PUT", "/topics/test-topic/subscriptions/test-sub")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .with_status(200)
        .with_body(r#"{"outcome":"created"}"#)
        .create();

    let result = admin
        .register_push_subscription(
            "test-topic",
            "test-sub",
            "https://example.com/webhook",
            None,
        )
        .await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.outcome, "created");
    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_register_push_subscription_with_boolean_filter() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);
    let admin = AdminClient::new(client);

    let mock = server
        .mock("PUT", "/topics/test-topic/subscriptions/test-sub")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .match_body(mockito::Matcher::JsonString(json!({
            "type": "push",
            "endpoint": "https://example.com/webhook",
            "filter": true
        }).to_string()))
        .with_status(200)
        .with_body(r#"{"outcome":"created"}"#)
        .create();

    let result = admin
        .register_push_subscription(
            "test-topic",
            "test-sub",
            "https://example.com/webhook",
            Some(Filter::Boolean(true)),
        )
        .await;

    assert!(result.is_ok());
    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_register_push_subscription_with_complex_filter() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);
    let admin = AdminClient::new(client);

    let complex_filter = ComplexFilter {
        filters: vec![
            FilterCondition {
                path: "data.type".to_string(),
                condition: "eq".to_string(),
                value: "user.created".to_string(),
            },
            FilterCondition {
                path: "data.user.premium".to_string(),
                condition: "eq".to_string(),
                value: "true".to_string(),
            },
        ],
        operator: "and".to_string(),
    };

    let mock = server
        .mock("PUT", "/topics/test-topic/subscriptions/test-sub")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .match_body(mockito::Matcher::JsonString(json!({
            "type": "push",
            "endpoint": "https://example.com/webhook",
            "filter": {
                "filters": [
                    {
                        "path": "data.type",
                        "condition": "eq",
                        "value": "user.created"
                    },
                    {
                        "path": "data.user.premium",
                        "condition": "eq",
                        "value": "true"
                    }
                ],
                "operator": "and"
            }
        }).to_string()))
        .with_status(200)
        .with_body(r#"{"outcome":"updated"}"#)
        .create();

    let result = admin
        .register_push_subscription(
            "test-topic",
            "test-sub",
            "https://example.com/webhook",
            Some(Filter::Complex(complex_filter)),
        )
        .await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.outcome, "updated");
    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_register_push_subscription_with_options() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);
    let admin = AdminClient::new(client);

    let options = PushSubscriptionOptions {
        filter: Some(Filter::Boolean(false)),
        rate_limit: Some("10/minute".to_string()),
        deduplication: Some("5m".to_string()),
    };

    let mock = server
        .mock("PUT", "/topics/test-topic/subscriptions/test-sub")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .match_body(mockito::Matcher::JsonString(json!({
            "type": "push",
            "endpoint": "https://example.com/webhook",
            "filter": false,
            "rate_limit": "10/minute",
            "deduplication": "5m"
        }).to_string()))
        .with_status(200)
        .with_body(r#"{"outcome":"created"}"#)
        .create();

    let result = admin
        .register_push_subscription_with_options(
            "test-topic",
            "test-sub",
            "https://example.com/webhook",
            options,
        )
        .await;

    assert!(result.is_ok());
    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_register_push_subscription_with_minimal_options() {
    let mut server = mockito::Server::new_async().await;
    let client = create_test_client(&server);
    let admin = AdminClient::new(client);

    let options = PushSubscriptionOptions {
        filter: None,
        rate_limit: None,
        deduplication: Some("1h".to_string()),
    };

    let mock = server
        .mock("PUT", "/topics/test-topic/subscriptions/test-sub")
        .match_header("Authorization", "test-token")
        .match_header("x-source", "sailhouse-rust")
        .match_body(mockito::Matcher::JsonString(json!({
            "type": "push",
            "endpoint": "https://example.com/webhook",
            "deduplication": "1h"
        }).to_string()))
        .with_status(200)
        .with_body(r#"{"outcome":"none"}"#)
        .create();

    let result = admin
        .register_push_subscription_with_options(
            "test-topic",
            "test-sub",
            "https://example.com/webhook",
            options,
        )
        .await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.outcome, "none");
    mock.assert();
}