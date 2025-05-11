use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

pub mod admin;
pub use admin::AdminClient;

#[derive(Debug, Clone)]
pub struct SailhouseClient {
    pub(crate) client: Client,
    token: String,
    pub(crate) base_url: String,
}

#[derive(Serialize)]
pub struct PublishBody<T> {
    pub data: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_group_instance_id: Option<String>,
}

pub struct PublishBuilder<'a, T> {
    client: &'a SailhouseClient,
    topic: String,
    data: T,
    metadata: Option<std::collections::HashMap<String, String>>,
    send_at: Option<DateTime<Utc>>,
    wait_group_instance_id: Option<String>,
}

impl<'a, T: Serialize> PublishBuilder<'a, T> {
    pub(crate) fn new(client: &'a SailhouseClient, topic: &str, data: T) -> Self {
        Self {
            client,
            topic: topic.to_string(),
            data,
            metadata: None,
            send_at: None,
            wait_group_instance_id: None,
        }
    }

    /// Add metadata to the event
    pub fn with_metadata(mut self, metadata: std::collections::HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Schedule the event to be delivered at a specific time
    pub fn with_scheduled_time(mut self, send_at: DateTime<Utc>) -> Self {
        self.send_at = Some(send_at);
        self
    }

    /// Associate this event with a wait group
    pub fn with_wait_group(mut self, wait_group_id: String) -> Self {
        self.wait_group_instance_id = Some(wait_group_id);
        self
    }

    /// Send the event to the topic
    pub async fn send(self) -> Result<PublishResponse, reqwest::Error> {
        let send_at_str = self.send_at.map(|date| date.to_rfc3339());

        let req = self.client
            .client
            .post(format!("{}/topics/{}/events", self.client.base_url, self.topic))
            .json(&PublishBody {
                data: self.data,
                metadata: self.metadata,
                send_at: send_at_str,
                wait_group_instance_id: self.wait_group_instance_id,
            });

        let res = self.client.do_req(req).await?;
        if res.status().as_u16() != 201 {
            // Handle error
        }
        let response = res.json::<PublishResponse>().await?;
        Ok(response)
    }
}

pub struct GetOption {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub type TimeWindow = String;

pub struct WaitOptions {
    pub ttl: Option<TimeWindow>,
}

#[derive(Serialize, Deserialize)]
pub struct WaitGroupInstanceResponse {
    pub wait_group_instance_id: String,
}

pub struct WaitEvent<T> {
    pub topic: String,
    pub body: T,
    pub metadata: Option<std::collections::HashMap<String, String>>,
    pub send_at: Option<DateTime<Utc>>,
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

    pub async fn wait<T: Serialize + Clone>(
        &self,
        topic: &str,
        events: Vec<WaitEvent<T>>,
        options: Option<WaitOptions>,
    ) -> Result<(), reqwest::Error> {
        // Create wait group instance
        let req = self
            .client
            .post(format!("{}/waitgroups/instances", self.base_url))
            .json(&serde_json::json!({
                "topic": topic,
                "ttl": options.as_ref().and_then(|o| o.ttl.as_ref()),
            }));

        let res = self.do_req(req).await?;
        let instance = res.json::<WaitGroupInstanceResponse>().await?;
        let wait_group_instance_id = instance.wait_group_instance_id;

        // Process all events sequentially to avoid borrowing issues
        for wait_event in events {
            self.publish_internal(
                topic,
                wait_event.body,
                wait_event.metadata,
                wait_event.send_at,
                Some(wait_group_instance_id.clone()),
            )
            .await?;
        }

        // Mark wait group as in progress (no-op for processing)
        let req = self
            .client
            .put(format!(
                "{}/waitgroups/instances/{}/events",
                self.base_url, wait_group_instance_id
            ))
            .json(&serde_json::json!({}));

        let res = self.do_req(req).await?;
        if res.status().as_u16() < 200 || res.status().as_u16() >= 300 {
            // Handle error
        }

        Ok(())
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

    /// Create a new builder for publishing events.
    ///
    /// # Example
    /// ```no_run
    /// # use sailhouse::SailhouseClient;
    /// # use serde_json::json;
    /// # use std::collections::HashMap;
    /// # async fn example() -> Result<(), reqwest::Error> {
    /// # let client = SailhouseClient::new("token".to_string());
    /// let response = client.publish("my-topic", json!({"message": "Hello, world!"}))
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish<T: Serialize>(&self, topic: &str, data: T) -> PublishBuilder<T> {
        PublishBuilder::new(self, topic, data)
    }

    /// Internal method to publish events, used by the wait implementation
    async fn publish_internal<T: Serialize>(
        &self,
        topic: &str,
        data: T,
        metadata: Option<std::collections::HashMap<String, String>>,
        send_at: Option<DateTime<Utc>>,
        wait_group_instance_id: Option<String>,
    ) -> Result<PublishResponse, reqwest::Error> {
        let send_at_str = send_at.map(|date| date.to_rfc3339());

        let req = self
            .client
            .post(format!("{}/topics/{}/events", self.base_url, topic))
            .json(&PublishBody {
                data,
                metadata,
                send_at: send_at_str,
                wait_group_instance_id,
            });

        let res = self.do_req(req).await?;
        if res.status().as_u16() != 201 {
            // Handle error
        }
        let response = res.json::<PublishResponse>().await?;
        Ok(response)
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

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishResponse {
    pub id: String,
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
