use crate::SailhouseClient;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct RegisterResult {
    pub outcome: String, // "created", "updated", or "none"
}

#[derive(Debug, Serialize)]
pub struct FilterCondition {
    pub path: String,
    pub condition: String,
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct ComplexFilter {
    pub filters: Vec<FilterCondition>,
    pub operator: String,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Filter {
    Boolean(bool),
    Complex(ComplexFilter),
}

// Keep the old FilterOption for backward compatibility
#[derive(Debug, Serialize)]
pub struct FilterOption {
    pub path: String,
    pub value: String,
}

#[derive(Debug)]
pub struct PushSubscriptionOptions {
    pub filter: Option<Filter>,
    pub rate_limit: Option<String>,
    pub deduplication: Option<String>,
}

#[derive(Debug, Serialize)]
struct RegisterPushSubscriptionRequest {
    #[serde(rename = "type")]
    pub subscription_type: String,
    pub endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduplication: Option<String>,
}

#[derive(Debug)]
pub struct AdminClient {
    client: SailhouseClient,
}

impl AdminClient {
    pub fn new(client: SailhouseClient) -> Self {
        Self { client }
    }

    pub async fn register_push_subscription(
        &self,
        topic: &str,
        subscription: &str,
        endpoint: &str,
        filter: Option<Filter>,
    ) -> Result<RegisterResult, reqwest::Error> {
        let url = format!(
            "{}/topics/{}/subscriptions/{}",
            self.client.base_url, topic, subscription
        );

        let request_body = RegisterPushSubscriptionRequest {
            subscription_type: "push".to_string(),
            endpoint: endpoint.to_string(),
            filter,
            rate_limit: None,
            deduplication: None,
        };

        let req = self.client.client.put(&url).json(&request_body);

        let response = self.client.do_req(req).await?;
        let result = response.json::<RegisterResult>().await?;

        Ok(result)
    }

    pub async fn register_push_subscription_with_options(
        &self,
        topic: &str,
        subscription: &str,
        endpoint: &str,
        options: PushSubscriptionOptions,
    ) -> Result<RegisterResult, reqwest::Error> {
        let url = format!(
            "{}/topics/{}/subscriptions/{}",
            self.client.base_url, topic, subscription
        );

        let request_body = RegisterPushSubscriptionRequest {
            subscription_type: "push".to_string(),
            endpoint: endpoint.to_string(),
            filter: options.filter,
            rate_limit: options.rate_limit,
            deduplication: options.deduplication,
        };

        let req = self.client.client.put(&url).json(&request_body);

        let response = self.client.do_req(req).await?;
        let result = response.json::<RegisterResult>().await?;

        Ok(result)
    }
}
