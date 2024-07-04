use tracing::{event, field, span, Level};

use crate::USER_TASK;

/// An asynchronous Client to make Requests with.
///
/// This is a wrapper type over for reqwest Client made for convinience.
/// If you are creating a custom client with [`reqwest::Client::builder()`],
/// then use the [`new_with`](Self::new_with) method or call [`Into::into`]
#[derive(Clone)]
pub struct Client {
    pub inner: reqwest::Client,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        <reqwest::Client as std::fmt::Debug>::fmt(&self.inner, f)
    }
}

impl From<reqwest::Client> for Client {
    fn from(value: reqwest::Client) -> Self {
        Self { inner: value }
    }
}

impl TryFrom<reqwest::ClientBuilder> for Client {
    type Error = reqwest::Error;

    fn try_from(value: reqwest::ClientBuilder) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: value.build()?,
        })
    }
}

impl Client {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::new(),
        }
    }

    pub fn new_with(client: reqwest::Client) -> Self {
        Self { inner: client }
    }

    pub fn delete<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.delete(url).into()
    }

    pub fn get<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.get(url).into()
    }

    pub fn patch<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.patch(url).into()
    }

    pub fn execute(
        &self,
        request: reqwest::Request,
    ) -> impl futures::Future<Output = Result<reqwest::Response, reqwest::Error>> {
        self.inner.execute(request)
    }

    pub fn head<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.head(url).into()
    }

    pub fn post<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.post(url).into()
    }

    pub fn put<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.put(url).into()
    }

    pub fn request<U: reqwest::IntoUrl>(&self, method: reqwest::Method, url: U) -> RequestBuilder {
        self.inner.request(method, url).into()
    }
}

/// Wrapper over [`reqwest::RequestBuilder`].
#[must_use = "RequestBuilder does nothing until you 'send' it"]
pub struct RequestBuilder {
    inner: reqwest::RequestBuilder,
}

impl std::ops::Deref for RequestBuilder {
    type Target = reqwest::RequestBuilder;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl std::ops::DerefMut for RequestBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<reqwest::RequestBuilder> for RequestBuilder {
    fn from(value: reqwest::RequestBuilder) -> Self {
        Self { inner: value }
    }
}

impl RequestBuilder {
    pub fn body<T: Into<reqwest::Body>>(mut self, body: T) -> RequestBuilder {
        self.inner = self.inner.body(body);
        self
    }

    pub async fn send(self) -> Result<reqwest::Response, reqwest::Error> {
        let (client, request) = self.inner.build_split();
        let request = request?;
        send_request(request, client).await
    }
}

/// Send a reqwest Request and emit traces for crate's tracing layer.
///
/// You can call this method directly with reqwest types without making use
/// of any wrapper types.
pub async fn send_request(
    request: reqwest::Request,
    client: reqwest::Client,
) -> Result<reqwest::Response, reqwest::Error> {
    let host = request.url().host();
    let path = request.url().path();
    let method = request.method();
    let span = span!(target: USER_TASK, Level::INFO, "reqwest", url = field::Empty, %path, %method);
    let _t = span.enter();
    if let Some(host) = host {
        span.record("url", field::display(host));
    }
    use http_body::Body as _;
    if let Some(size) = request.body().and_then(|x| x.size_hint().exact()) {
        event!(name: "sent.gauge", target: USER_TASK, Level::INFO, value = size as f64);
    }
    drop(_t);
    let resp = client.execute(request).await?;
    let _t = span.enter();
    if let Some(size) = resp.content_length() {
        event!(name: "receive.gauge", target: USER_TASK, Level::INFO, value = size as f64);
    }
    event!(name: "status.counter", target: USER_TASK, Level::INFO, status = resp.status().as_str(), value = 1u64);
    Ok(resp)
}
