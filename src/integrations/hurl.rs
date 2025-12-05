use std::{any, collections::HashMap};

use axum::http::HeaderName;
use bytes::Bytes;
use reqwest::{header::HeaderValue, Method, StatusCode};
use url::Url;

use crate::{data::RuntimeDataStore, user::AsyncUserBuilder};

#[derive(Debug, Default)]
pub struct VariableRegistry {
    pub variables: HashMap<String, Box<dyn any::Any + Send + Sync>>,
}

#[derive(Debug, PartialEq, Eq)]
enum MayLookup<T> {
    Static(T),
    LookupRequire(String),
    LookupOptional(String),
    LookupDefault { name: String, default: T },
}

impl<T> MayLookup<T> {
    pub fn get_static(&self) -> Option<&T> {
        match self {
            MayLookup::Static(value) => Some(value),
            _ => None,
        }
    }
}

impl<T: Clone> Clone for MayLookup<T> {
    fn clone(&self) -> Self {
        match self {
            MayLookup::Static(value) => MayLookup::Static(value.clone()),
            MayLookup::LookupRequire(name) => MayLookup::LookupRequire(name.clone()),
            MayLookup::LookupOptional(name) => MayLookup::LookupOptional(name.clone()),
            MayLookup::LookupDefault { name, default } => MayLookup::LookupDefault {
                name: name.clone(),
                default: default.clone(),
            },
        }
    }
}

#[derive(Debug)]
struct HurlUserBuilder {
    entries: Vec<HurlEntry>,
    user_entries: Vec<HurlEntry>,
}

struct HurlUser {
    variables: VariableRegistry,
    entries: Vec<HurlEntry>,
}

#[derive(Debug, Clone)]
pub struct Options {
    aws_sigv4: Option<String>,
    cacert: Option<String>,
    cert: Option<String>,
    key: Option<String>,
    compressed: bool,
    connect_timeout: Option<std::time::Duration>,
    delay: Option<std::time::Duration>,
    http3: bool,
    insecure: bool,
    ipv6: bool,
    limit_rate: Option<u64>,
    location: bool,
    max_redirs: Option<u32>,
    max_time: Option<std::time::Duration>,
    output: Option<String>,
    path_as_is: bool,
    retry: Option<u32>,
    retry_interval: Option<std::time::Duration>,
    skip: bool,
    unix_socket: Option<String>,
    user: Option<String>,
    proxy: Option<String>,
    variables: HashMap<String, String>,
    verbose: bool,
    very_verbose: bool,
}

#[derive(Debug, Clone)]
struct HurlRequest {
    method: Method,
    url: Url,
    options: Options,
    headers: Vec<(HeaderName, MayLookup<HeaderValue>)>,
    form: Option<Vec<(String, MayLookup<String>)>>,
    query: Option<Vec<(String, MayLookup<String>)>>,
    basicauth: Option<(String, Option<String>)>,
    cookies: Vec<(String, MayLookup<HeaderValue>)>,
    body: MayLookup<bytes::Bytes>,
}

#[derive(Debug, Clone)]
enum ResponseSectionValue {
    Capture(hurl_core::ast::Capture),
    Asserts(hurl_core::ast::Assert),
}

#[derive(Debug, Clone)]
struct HurlResponse {
    status: StatusCode,
    headers: Vec<(HeaderName, MayLookup<HeaderValue>)>,
    body: MayLookup<bytes::Bytes>,
    sections: Vec<ResponseSectionValue>,
}

#[derive(Debug, Clone)]
struct HurlEntry {
    request: HurlRequest,
    response: HurlResponse,
}

#[async_trait::async_trait]
impl<'a> AsyncUserBuilder<'a> for std::sync::Arc<HurlUserBuilder> {
    type Output = HurlUser;
    async fn build(
        &self,
        _store: &'a RuntimeDataStore,
    ) -> Result<Self::Output, crate::error::Error> {
        let mut vars = VariableRegistry::default();
        for item in &self.entries {
            let this_vars = run_entry(&item, &vars).await?;
            vars.variables.extend(this_vars.variables);
        }
        Ok(HurlUser {
            variables: vars,
            entries: self.user_entries.clone(),
        })
    }
}

impl crate::user::User for HurlUser {
    fn call(&mut self) -> impl std::future::Future<Output = crate::UserResult> + std::marker::Send {
        futures::future::ready(Ok(()))
    }
}

async fn run_entry(
    entry: &HurlEntry,
    vars: &VariableRegistry,
) -> Result<VariableRegistry, anyhow::Error> {
    let this_vars = VariableRegistry::default();
    let hurl_request = entry.request.clone();
    let hurl_response = entry.response.clone();
    let client = reqwest::Client::new();
    let builder = client.request(hurl_request.method.clone(), hurl_request.url.clone());
    let req = forge_request(&hurl_request, builder, vars)?;
    let resp = crate::client::reqwest::RequestBuilder::from(req)
        .send()
        .await?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.bytes().await?;

    check_core_asserts(status, &headers, &body, &hurl_response)?;

    Ok(this_vars)
}

async fn run_entry_non_telemetry(
    entry: &HurlEntry,
    vars: &VariableRegistry,
) -> Result<VariableRegistry, anyhow::Error> {
    let this_vars = VariableRegistry::default();
    let hurl_request = entry.request.clone();
    let hurl_response = entry.response.clone();
    let client = reqwest::Client::new();
    let builder = client.request(hurl_request.method.clone(), hurl_request.url.clone());
    let req = forge_request(&hurl_request, builder, vars)?;
    let resp = req.send().await?;

    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.bytes().await?;

    check_core_asserts(status, &headers, &body, &hurl_response)?;

    Ok(this_vars)
}

fn check_core_asserts(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    body: &Bytes,
    hurl_response: &HurlResponse,
) -> anyhow::Result<()> {
    if hurl_response.status != status {
        return Err(anyhow::anyhow!(
            "Status code mismatch - expected {} actual {}",
            hurl_response.status,
            status
        ));
    }

    for (header_name, header_value) in hurl_response.headers.iter() {
        let header_value = header_value
            .get_static()
            .ok_or_else(|| anyhow::anyhow!("Base check header value is not statically defined"))?;
        let resp_header = headers.get(header_name).ok_or_else(|| {
            anyhow::anyhow!(
                "Header not found in the response - expected header {}",
                header_name
            )
        })?;
        if resp_header != header_value {
            return Err(anyhow::anyhow!(
                "Header mismatch ({}) - expected {:?} actual {:?}",
                header_name.as_str(),
                header_value,
                resp_header
            ));
        }
    }

    let expected_response = hurl_response
        .body
        .get_static()
        .ok_or_else(|| anyhow::anyhow!("Base check response body is not statically defined"))?;
    let actual_response = body;
    if expected_response != actual_response {
        return Err(anyhow::anyhow!(
            "Body mismatch - expected {} actual {}",
            String::from_utf8_lossy(expected_response),
            String::from_utf8_lossy(actual_response)
        ));
    }

    Ok(())
}

fn map_lookup<'a, T: any::Any>(
    var: &'a MayLookup<T>,
    vars: &'a VariableRegistry,
) -> Result<Option<&'a T>, ()> {
    match var {
        MayLookup::Static(x) => Ok(Some(x)),
        MayLookup::LookupRequire(key) => vars
            .variables
            .get(key)
            .map(|x| x.as_ref().downcast_ref::<T>().unwrap())
            .map(Some)
            .ok_or(()),
        MayLookup::LookupOptional(key) => vars
            .variables
            .get(key)
            .map(|x| x.as_ref().downcast_ref::<T>().unwrap())
            .map(Some)
            .ok_or(()),
        MayLookup::LookupDefault { name: key, default } => Ok(vars
            .variables
            .get(key)
            .map(|x| x.as_ref().downcast_ref::<T>().unwrap())
            .or(Some(default))),
    }
}

fn forge_request(
    request: &HurlRequest,
    mut builder: reqwest::RequestBuilder,
    vars: &VariableRegistry,
) -> Result<reqwest::RequestBuilder, anyhow::Error> {
    for (name, value) in &request.headers {
        let val = map_lookup(value, vars)
            .map_err(|_| anyhow::anyhow!("Missing required header: {}", name))?;
        if let Some(val) = val {
            builder = builder.header(name, val.clone());
        }
    }

    if let Some(form) = &request.form {
        let mut resolved_form = vec![];
        for (name, value) in form {
            let val = map_lookup(value, vars)
                .map_err(|_| anyhow::anyhow!("Missing required form field: {}", name))?;
            if let Some(val) = val {
                resolved_form.push((name, val));
            }
        }
        builder = builder.form(&resolved_form);
    }

    if let Some(query) = &request.query {
        let mut resolved_query = vec![];
        for (name, value) in query {
            let val = map_lookup(value, vars)
                .map_err(|_| anyhow::anyhow!("Missing required query field: {}", name))?;
            if let Some(val) = val {
                resolved_query.push((name, val));
            }
        }
        builder = builder.query(&resolved_query);
    }

    if let Some(basicauth) = &request.basicauth {
        builder = builder.basic_auth(&basicauth.0, basicauth.1.as_ref());
    }

    let body =
        map_lookup(&request.body, vars).map_err(|_| anyhow::anyhow!("Missing body field"))?;
    if let Some(body) = body {
        builder = builder.body(body.clone());
    }

    // !todo cookies

    Ok(builder)
}
