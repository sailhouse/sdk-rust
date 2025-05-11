use std::{collections::HashMap, env};

fn main() {
    let token = match env::var("SAILHOUSE_TOKEN") {
        Ok(value) => value,
        Err(error) => {
            eprintln!("Failed to read SAILHOUSE_TOKEN: {}", error);
            return;
        }
    };

    let client = sailhouse::SailhouseClient::new(token);
    let mut data = HashMap::new();
    data.insert("message", "Hello world!");

    let publish_future = client.publish("example-topic", data).send();
    let _ = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(publish_future);
}
