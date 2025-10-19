use extism_pdk::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize)]
struct FilterContext {
    author_did: String,
    #[serde(default)]
    post: Option<Post>,
    #[serde(default)]
    repost: Option<Post>,
    follows: HashMap<String, i64>,
    followed: HashMap<String, i64>,
}

#[derive(Deserialize)]
struct Post {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
}

/// Simple keyword filter
/// Returns true if the post contains any of the specified keywords
#[plugin_fn]
pub fn filter(ctx: Json<FilterContext>) -> FnResult<Json<bool>> {
    let keywords = vec!["rust", "wasm", "golang", "bluesky", "atproto"];

    let allowed = if let Some(post) = &ctx.0.post {
        if let Some(text) = &post.text {
            let text_lower = text.to_lowercase();
            keywords.iter().any(|keyword| text_lower.contains(keyword))
        } else {
            // Posts without text (e.g., image-only posts) are not allowed
            false
        }
    } else {
        false
    };

    Ok(Json(allowed))
}
