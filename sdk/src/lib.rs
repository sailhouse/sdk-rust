use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct SailhouseClient {
    client: Client,
    token: String,
    base_url: String,
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
        SailhouseClient {
            client,
            token,
            base_url: "https://api.sailhouse.dev".to_string(),
        }
    }

    pub fn with_base_url(client: Client, token: String, base_url: String) -> Self {
        SailhouseClient {
            client,
            token,
            base_url,
        }
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
            "{}/topics/{}/subscriptions/{}/events",
            self.base_url, topic, subscription
        ));

        if let Some(limit) = opts.limit {
            req = req.query(&[("limit", limit.to_string())]);
        }
        if let Some(offset) = opts.offset {
            req = req.query(&[("offset", offset.to_string())]);
        }

        let res = self.do_req(req).await?;
        let mut response_body = res.json::<GetEventsResponse>().await?;

        // set topic and subscription for each event
        let mut events = response_body.events;
        for event in events.iter_mut() {
            event.topic = topic.to_string();
            event.subscription = subscription.to_string();
            event.client = Some(self.clone());
        }

        response_body.events = events;

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
                "{}/topics/{}/subscriptions/{}/events/{}",
                self.base_url, topic, subscription, id
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
            .post(format!("{}/topics/{}/events", self.base_url, topic))
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
    #[serde(skip)]
    pub topic: String,
    #[serde(skip)]
    pub subscription: String,

    #[serde(skip)]
    pub client: Option<SailhouseClient>,
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
