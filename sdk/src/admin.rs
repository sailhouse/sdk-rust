use crate::SailhouseClient;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct RegisterResult {
    pub outcome: String, // "created", "updated", or "none"
}

#[derive(Debug, Serialize)]
pub struct FilterOption {
    pub path: String,
    pub value: String,
}

#[derive(Debug, Serialize)]
struct RegisterPushSubscriptionRequest {
    #[serde(rename = "type")]
    pub subscription_type: String,
    pub endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterOption>,
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
        filter: Option<FilterOption>,
    ) -> Result<RegisterResult, reqwest::Error> {
        let url = format!(
            "{}/topics/{}/subscriptions/{}",
            self.client.base_url, topic, subscription
        );

        let request_body = RegisterPushSubscriptionRequest {
            subscription_type: "push".to_string(),
            endpoint: endpoint.to_string(),
            filter,
        };

        let req = self.client.client.put(&url).json(&request_body);

        let response = self.client.do_req(req).await?;
        let result = response.json::<RegisterResult>().await?;

        Ok(result)
    }
}