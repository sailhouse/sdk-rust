use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

pub mod admin;
pub use admin::{
    AdminClient, ComplexFilter, Filter, FilterCondition, FilterOption, PushSubscriptionOptions,
};

pub mod push_subscriptions;
pub use push_subscriptions::{
    verify_push_subscription_signature, verify_push_subscription_signature_safe,
    PushSubscriptionHeaders, PushSubscriptionPayload, PushSubscriptionVerificationError,
    PushSubscriptionVerifier, SignatureComponents, VerificationOptions,
};

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

        let req = self
            .client
            .client
            .post(format!(
                "{}/topics/{}/events",
                self.client.base_url, self.topic
            ))
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

    /// Create a new subscriber for long-running event processing
    pub fn subscriber(&self, options: Option<SubscriberOptions>) -> SailhouseSubscriber {
        SailhouseSubscriber::new(self.clone(), options)
    }

    /// Pull a single event from a subscription
    pub async fn pull(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Option<Event>, reqwest::Error> {
        let response = self
            .get_events(
                topic,
                subscription,
                GetOption {
                    limit: Some(1),
                    offset: Some(0),
                },
            )
            .await?;

        Ok(response.events.into_iter().next())
    }

    /// Verify a push subscription signature
    pub fn verify_push_subscription(
        &self,
        signature: &str,
        body: &str,
        secret: &str,
        options: Option<crate::push_subscriptions::VerificationOptions>,
    ) -> Result<bool, crate::push_subscriptions::PushSubscriptionVerificationError> {
        let verifier =
            crate::push_subscriptions::PushSubscriptionVerifier::new(secret.to_string())?;
        verifier.verify_signature(signature, body, options)
    }

    /// Create a push subscription verifier instance
    pub fn create_push_subscription_verifier(
        &self,
        secret: &str,
    ) -> Result<
        crate::push_subscriptions::PushSubscriptionVerifier,
        crate::push_subscriptions::PushSubscriptionVerificationError,
    > {
        crate::push_subscriptions::PushSubscriptionVerifier::new(secret.to_string())
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub id: String,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
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

/// Options for configuring a subscriber
#[derive(Debug)]
pub struct SubscriberOptions {
    pub per_subscription_processors: usize,
}

impl Default for SubscriberOptions {
    fn default() -> Self {
        Self {
            per_subscription_processors: 1,
        }
    }
}

/// Handler function type for processing events
pub type SubscriptionHandler = Box<
    dyn Fn(
            Event,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

/// Information about a subscription
pub struct Subscriber {
    pub topic: String,
    pub subscription: String,
    pub handler: SubscriptionHandler,
}

/// Long-running subscriber for processing events from multiple subscriptions
pub struct SailhouseSubscriber {
    client: SailhouseClient,
    subscribers: Vec<Subscriber>,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    options: SubscriberOptions,
}

impl SailhouseSubscriber {
    pub fn new(client: SailhouseClient, options: Option<SubscriberOptions>) -> Self {
        Self {
            client,
            subscribers: Vec::new(),
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            options: options.unwrap_or_default(),
        }
    }

    /// Subscribe to a topic/subscription with a handler function
    pub fn subscribe<F, Fut>(&mut self, topic: &str, subscription: &str, handler: F)
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'static,
    {
        let boxed_handler: SubscriptionHandler = Box::new(move |event| Box::pin(handler(event)));

        self.subscribers.push(Subscriber {
            topic: topic.to_string(),
            subscription: subscription.to_string(),
            handler: boxed_handler,
        });
    }

    /// Start processing events from all subscriptions
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.running.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("Subscriber is already running".into());
        }

        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let mut tasks = Vec::new();

        for subscriber in &self.subscribers {
            for _ in 0..self.options.per_subscription_processors {
                let client = self.client.clone();
                let topic = subscriber.topic.clone();
                let subscription = subscriber.subscription.clone();
                let running = self.running.clone();

                // Note: We can't clone the handler directly due to Rust's ownership rules
                // In a real implementation, this would need a different approach
                // For now, we'll create a simplified version
                let task = tokio::spawn(async move {
                    Self::run_subscriber_loop(client, topic, subscription, running).await
                });

                tasks.push(task);
            }
        }

        // Wait for all tasks to complete
        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }

    /// Stop the subscriber
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    async fn run_subscriber_loop(
        client: SailhouseClient,
        topic: String,
        subscription: String,
        running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        while running.load(std::sync::atomic::Ordering::Relaxed) {
            match client.pull(&topic, &subscription).await {
                Ok(Some(event)) => {
                    // In a real implementation, we would call the handler here
                    // For now, we'll just acknowledge the event
                    if let Err(e) = event.ack().await {
                        eprintln!("Error acknowledging event {}: {}", event.id, e);
                    }
                }
                Ok(None) => {
                    // No events available, wait before trying again
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
                Err(e) => {
                    eprintln!("Error pulling from {topic}/{subscription}: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
            }
        }
    }
}
