use std::env;

fn main() {
    let token = match env::var("SAILHOUSE_TOKEN") {
        Ok(value) => value,
        Err(error) => {
            eprintln!("Failed to read SAILHOUSE_TOKEN: {}", error);
            return;
        }
    };

    let client = sailhouse::SailhouseClient::new(token);
    let get_future = client.get_events(
        "example-topic",
        "example-subscription-sdk-rust",
        sailhouse::GetOption {
            limit: Some(10),
            offset: Some(0),
        },
    );
    let result = tokio::runtime::Runtime::new().unwrap().block_on(get_future);

    match result {
        Ok(response) => {
            response.events.iter().for_each(|event| {
                println!("Event: {:?}", event.data);
                let ack_future = event.ack();
                let _ = tokio::runtime::Runtime::new().unwrap().block_on(ack_future);
            });
        }
        Err(error) => {
            eprintln!("Failed to get events: {}", error);
        }
    }
}
