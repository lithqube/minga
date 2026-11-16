use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Weak};

use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use tokio::sync::{Mutex, OnceCell};

const NATS_PORT: u16 = 4222;

static NATS: OnceCell<Mutex<Option<Weak<NatsServer>>>> = OnceCell::const_new();

pub struct NatsServer {
    url: String,
    _container: Option<ContainerAsync<GenericImage>>,
}

impl Deref for NatsServer {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.url
    }
}

impl AsRef<str> for NatsServer {
    fn as_ref(&self) -> &str {
        &self.url
    }
}

impl fmt::Display for NatsServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.url)
    }
}

/// Return the configured NATS server, or `None` when Docker is unavailable.
///
/// Setting `MINGA_REQUIRE_NATS=1` turns an unavailable server into a hard
/// failure, as used by the dedicated NATS CI job.
pub async fn nats_url() -> Option<Arc<NatsServer>> {
    if let Ok(url) = std::env::var("MINGA_TEST_NATS_URL") {
        return Some(Arc::new(NatsServer {
            url,
            _container: None,
        }));
    }

    let state = NATS.get_or_init(|| async { Mutex::new(None) }).await;
    let mut state = state.lock().await;
    if let Some(server) = state.as_ref().and_then(Weak::upgrade) {
        return Some(server);
    }

    let image = GenericImage::new("nats", "2.15.3-alpine")
        .with_exposed_port(NATS_PORT.into())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(vec!["-js"]);
    let container = match image.start().await {
        Ok(container) => container,
        Err(error) => {
            return unavailable(
                format!("cannot start NATS container: {error}"),
                required_nats(),
            )
        }
    };
    let port = match container.get_host_port_ipv4(NATS_PORT).await {
        Ok(port) => port,
        Err(error) => {
            return unavailable(
                format!("cannot resolve NATS port: {error}"),
                required_nats(),
            )
        }
    };

    let server = Arc::new(NatsServer {
        url: format!("nats://127.0.0.1:{port}"),
        _container: Some(container),
    });
    *state = Some(Arc::downgrade(&server));
    Some(server)
}

fn required_nats() -> bool {
    std::env::var("MINGA_REQUIRE_NATS").as_deref() == Ok("1")
}

fn unavailable<T>(reason: String, required: bool) -> Option<T> {
    if required {
        panic!("{reason}; set MINGA_TEST_NATS_URL to a reachable JetStream server");
    }
    eprintln!("skipping: {reason}");
    None
}

#[cfg(test)]
mod tests {
    use super::unavailable;

    #[test]
    fn required_server_unavailability_panics_instead_of_skipping() {
        let result =
            std::panic::catch_unwind(|| unavailable::<()>("test server missing".into(), true));
        assert!(result.is_err());
    }
}
