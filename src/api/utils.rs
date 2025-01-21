use super::{
    errors::{ApiError, ApiErrorType, OverloadedError},
    handlers::DbgPaths,
    responses::{common_error_reply, json_serialize_embed, CallResponse, JsonReply},
};
use crate::utils::{ApiKeys, RoutesPoWInfo, StringError};
use futures::Future;
use moka::future::{Cache, CacheBuilder};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{log::error, warn};
use warp::{
    hyper::{HeaderMap, StatusCode},
    path::FullPath,
    Filter, Rejection, Reply,
};

// Clone component/struct to use in route
pub fn with_node_component<T: Clone + Send>(
    comp: T,
) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || comp.clone())
}

// Add route path to mutable reference DbgPaths
pub fn warp_path(
    dp: &mut DbgPaths,
    p: &'static str,
) -> impl Filter<Extract = (), Error = Rejection> + Clone {
    dp.push(p);
    warp::path(p)
}

// Maps an error that implements `ToString` to JsonReply error for bad requests.
pub fn map_string_err<T: ToString>(r: CallResponse, e: T, s: StatusCode) -> JsonReply {
    r.into_err(s, ApiErrorType::Generic(e.to_string()))
        .unwrap_err() // Should panic if result is not Err
}

// Maps an error that implements `ToString` to JsonReply error for bad requests.
pub fn map_to_string_err<T: ToString>(e: T) -> StringError {
    StringError(e.to_string())
}

// Map API response from Result<JsonReply, JsonReply> to Result<warp::Reply, warp::Rejection>
//Adds responses to a cache
pub fn map_api_res_and_cache(
    call_id: String,
    cache: ReplyCache,
    r: impl Future<Output = Result<JsonReply, JsonReply>>,
) -> impl Future<Output = Result<impl warp::Reply, warp::Rejection>> {
    use futures::future::TryFutureExt;
    let r_2 = get_or_insert_cache_value(call_id, cache, r);
    r_2.map_ok_or_else(Ok, Ok)
}

// Authorizes a request based on API keys as well as PoW requirements for the route
// Successfull authorization will extract the x-cache-id header value
pub fn auth_request(
    _routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
) -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::path::full()
        .and(warp::header::headers_cloned())
        .and_then(move |path: FullPath, headers: HeaderMap| {
            let route_path = path.as_str()[1..].to_owned(); /* Slice to remove '/' prefix */
            // let route_difficulty = routes_pow.lock().unwrap().get(&route_path).cloned();
            let needed_keys = api_keys.lock().unwrap().get(&route_path).cloned();

            async move {
                // Extract headers
                let id = headers
                    .get("x-cache-id")
                    .and_then(|n| n.to_str().ok())
                    .unwrap_or_default();

                let _nonce = headers
                    .get("x-nonce")
                    .and_then(|n| n.to_str().ok())
                    .unwrap_or_default();

                let api_key = headers
                    .get("x-api-key")
                    .and_then(|n| n.to_str().ok())
                    .unwrap_or_default();

                // Error for authorization failure
                let err_unauthorized = Err(warp::reject::custom(ApiError::new(
                    StatusCode::UNAUTHORIZED,
                    ApiErrorType::Unauthorized,
                    id.to_owned(),
                    route_path,
                )));

                // All requests require a unique ID of 32 characters
                // if id.chars().count() != 32 {
                //     return err_unauthorized;
                // }

                // API key is needed
                if let Some(needed_api_keys) = needed_keys {
                    if !needed_api_keys.contains(&api_key.to_string()) {
                        warn!("Unauthorized - API Key needed");
                        return err_unauthorized;
                    }
                }

                // let hash_content = format!("{nonce}-{id}");

                // This route requires PoW
                // if let Some(difficulty) = route_difficulty {
                //     if validate_pow_for_diff(difficulty, hash_content.as_bytes()).is_none() {
                //         return err_unauthorized;
                //     }
                // }

                // No PoW required
                Ok(id.to_owned())
            }
        })
        .or_else(move |err| async move { Err(err) })
}

// Custom function to handle request errors that occur before the request can be logically processed
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    let mut error = ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        ApiErrorType::InternalError,
        "null".to_string(),
        "null".to_string(),
    );

    if err.is_not_found() {
        // Method not found
        error.code = StatusCode::NOT_FOUND;
        error.message = ApiErrorType::MethodNotFound;
    } else if err
        .find::<warp::filters::body::BodyDeserializeError>()
        .is_some()
    {
        // Failure to deserialize request body
        error.code = StatusCode::BAD_REQUEST;
        error.message = ApiErrorType::BadRequest;
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        // Method not allowed
        error.code = StatusCode::METHOD_NOT_ALLOWED;
        error.message = ApiErrorType::MethodNotAllowed;
    } else if let Some(err) = err.find::<ApiError>().cloned() {
        // Custom errors
        error = err;
    } else {
        // This should not happen! All errors should be handled
        error!("Unhandled API rejection: {:?}", err);
        error.code = StatusCode::INTERNAL_SERVER_ERROR;
        error.message =
            ApiErrorType::Generic(format!("Unhandled rejection. Internal Server Error"));
    }

    Ok(common_error_reply(
        error.code,
        error.message,
        &error.id,
        &error.route,
        json_serialize_embed("null"),
    ))
}

//Cache data type, live time and maximum size
pub type ReplyCache = Cache<String, Result<JsonReply, JsonReply>>;
pub const CACHE_LIVE_TIME: u64 = 60 * 60;
pub const MAX_RESPONSE_CACHE_SIZE: u64 = 10000;

//Create a cache with items that expire and are removed after a set period of time
pub fn create_new_cache(time_to_live: u64) -> ReplyCache {
    CacheBuilder::new(MAX_RESPONSE_CACHE_SIZE)
        //Time to live: each element is valid for time_to_live seconds - deleted time_to_live seconds after insertion
        .time_to_live(Duration::from_secs(time_to_live))
        // Create the cache.
        .build()
}

//gets cache value from BTreeMap. Clears old values if 24 hours has passed since the last clear.
//ReplyCache is a moka::future::cache of type <String, Result<JsonReply, JsonReply>>
fn get_cache_value(call_id: &str, cache: &ReplyCache) -> Option<Result<JsonReply, JsonReply>> {
    cache.get(&String::from(call_id))
}

//inserts cache value into BTreeMap. Clears old values if 24 hours has passed since the last clear.
//ReplyCache is a moka::future::cache of type <String, Result<JsonReply, JsonReply>>
async fn insert_cache_value(
    call_id: &str,
    response: Result<JsonReply, JsonReply>,
    cache: &ReplyCache,
) -> Result<JsonReply, JsonReply> {
    if !call_id.is_empty() {
        cache.insert(String::from(call_id), response.clone()).await;
    }

    response
}

pub async fn get_or_insert_cache_value(
    call_id: String,
    cache: ReplyCache,
    r: impl Future<Output = Result<JsonReply, JsonReply>>,
) -> Result<JsonReply, JsonReply> {
    let fetched_value = get_cache_value(&call_id, &cache);
    if let Some(value) = fetched_value {
        return value;
    }

    insert_cache_value(&call_id, r.await, &cache).await
}

/// Filter to limit the number of concurrent requests.
///
/// # Arguments
///
/// * `semaphore` - A reference to a semaphore that limits the number of concurrent requests.
pub fn with_semaphore(
    semaphore: Arc<Semaphore>,
) -> impl Filter<Extract = ((),), Error = warp::Rejection> + Clone {
    warp::any().and_then(move || {
        let semaphore = semaphore.clone();
        async move {
            let _permit = semaphore
                .try_acquire()
                .map_err(|_| warp::reject::custom(OverloadedError))?;
            Ok::<(), warp::Rejection>(())
        }
    })
}
