use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug)]
pub struct SailhouseClient {
    client: Client,
    token: String,
}

#[derive(Serialize)]
pub struct PublishBody<T> {
    pub data: T,
}

pub struct GetOption {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl SailhouseClient {
    pub fn new(token: String) -> Self {
        Self::with_options(Client::new(), token)
    }

    pub fn with_options(client: Client, token: String) -> Self {
        SailhouseClient { client, token }
    }

    async fn do_req(&self, req: reqwest::RequestBuilder) -> reqwest::Result<reqwest::Response> {
        req.header("Authorization", &self.token)
            .header("x-source", "sailhouse-rust")
            .send()
            .await
    }

    pub async fn get_events(
        &self,
        topic: &str,
        subscription: &str,
        opts: GetOption,
    ) -> Result<GetEventsResponse, reqwest::Error> {
        let mut req = self.client.get(format!(
            "https://api.sailhouse.dev/topics/{}/subscriptions/{}/events",
            topic, subscription
        ));

        if let Some(limit) = opts.limit {
            req = req.query(&[("limit", limit.to_string())]);
        }
        if let Some(offset) = opts.offset {
            req = req.query(&[("offset", offset.to_string())]);
        }
        if let Some(time_window) = opts.time_window {
            req = req.query(&[("time_window", time_window.as_secs().to_string())]);
        }

        let res = self.do_req(req).await?;
        let response_body: GetEventsResponse = res.json().await?;
        Ok(response_body)
    }

    pub async fn acknowledge_message(
        &self,
        topic: &str,
        subscription: &str,
        id: &str,
    ) -> Result<(), reqwest::Error> {
        let req = self
            .client
            .post(format!(
                "https://api.sailhouse.dev/topics/{}/subscriptions/{}/events/{}",
                topic, subscription, id
            ))
            .json(&serde_json::json!({}));

        let res = self.do_req(req).await?;
        if res.status().as_u16() != 200 {
            // Handle error
        }
        Ok(())
    }

    pub async fn publish<T: Serialize>(&self, topic: &str, data: T) -> Result<(), reqwest::Error> {
        let req = self
            .client
            .post(format!("https://api.sailhouse.dev/topics/{}/events", topic))
            .json(&PublishBody { data });

        let res = self.do_req(req).await?;
        if res.status().as_u16() != 201 {
            // Handle error
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct GetEventsResponse {
    pub events: Vec<Event>,
    pub offset: i32,
    pub limit: i32,
}

#[derive(Debug, Deserialize)]
pub struct EventResponse {
    pub id: String,
    pub data: Value,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub id: String,
    pub data: Value,
    topic: String,
    subscription: String,
    #[serde(skip)]
    client: Option<SailhouseClient>,
}

impl Event {
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.data.clone())
    }

    pub async fn ack(&self) -> Result<(), reqwest::Error> {
        match &self.client {
            Some(client) => {
                client
                    .acknowledge_message(&self.topic, &self.subscription, &self.id)
                    .await
            }
            None => Ok(()),
        }
    }
}
