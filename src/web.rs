//! The web page that lists the services. Each service also has a page of
//! its own that publishes its `cop://` URI in a `link rel="cassis-order"`,
//! so a wallet can be pointed at a web address instead of a QR code.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use crate::cop::Service;
use std::sync::Arc;

pub struct Listing {
    pub slug: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub uri: String,
    pub service: Arc<dyn Service>,
}

const STYLE: &str = "\
:root{color-scheme:light dark;--bg:#fafafa;--fg:#1c1c1e;--card:#fff;--line:#ddd;--accent:#6b3fa0}\
@media(prefers-color-scheme:dark){:root{--bg:#161618;--fg:#eee;--card:#222226;--line:#38383d;--accent:#b48ae8}}\
body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif}\
main{max-width:40rem;margin:0 auto;padding:2rem 1rem}\
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:1rem 1.25rem;margin:1rem 0}\
h1{margin:0 0 .25rem}h2{margin:0}a{color:var(--accent)}\
code{display:block;overflow-wrap:anywhere;font-size:.8rem;margin:.5rem 0}\
.qr svg{width:12rem;height:12rem;background:#fff;border-radius:8px}\
.muted{opacity:.7}\
.canvas{display:block;width:100%;image-rendering:pixelated;border:1px solid var(--line);border-radius:8px}\
.message{white-space:pre-wrap;overflow-wrap:anywhere}\
.titled{display:flex;align-items:center;gap:.45em}\
.icon{width:1.25em;height:1.25em;flex:none}";

pub fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn document(head: &str, body: &str) -> Html<String> {
    Html(format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <title>Cassis demo stores</title><style>{STYLE}</style>{head}</head>\
         <body><main>{body}</main></body></html>"
    ))
}

fn qr(uri: &str) -> String {
    qrcode::QrCode::new(uri)
        .map(|code| code.render::<qrcode::render::svg::Color>().quiet_zone(true).build())
        .unwrap_or_default()
}

async fn index(State(listings): State<Arc<Vec<Listing>>>) -> Html<String> {
    let mut body = String::from("<h1>Cassis demo stores</h1>");
    for listing in listings.iter() {
        body.push_str(&format!(
            "<div class=\"card\"><h2 class=\"titled\"><img class=\"icon\" src=\"{icon}\" alt=\"\">{name}</h2><p>{description}</p>\
             <p><a href=\"{uri}\">Open</a> · <a href=\"/{slug}\">QR code</a></p>\
             <code>{uri}</code></div>",
            icon = escape(&listing.service.icon().url()),
            name = listing.name,
            description = listing.description,
            uri = escape(&listing.uri),
            slug = listing.slug,
        ));
    }
    document("", &body)
}

async fn service(
    State(listings): State<Arc<Vec<Listing>>>,
    Path(slug): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let listing = listings
        .iter()
        .find(|listing| listing.slug == slug)
        .ok_or(StatusCode::NOT_FOUND)?;
    let uri = escape(&listing.uri);
    let icon = escape(&listing.service.icon().url());
    let preview = listing.service.preview();
    // What the service holds changes as people pay: look again now and then.
    let refresh = if preview.is_empty() {
        ""
    } else {
        "<meta http-equiv=\"refresh\" content=\"15\">"
    };
    Ok(document(
        &format!(
            "<link rel=\"cassis-order\" href=\"{uri}\"><link rel=\"icon\" href=\"{icon}\">{refresh}",
        ),
        &format!(
            "<p><a href=\"/\">All stores</a></p><h1 class=\"titled\"><img class=\"icon\" src=\"{icon}\" alt=\"\">{name}</h1><p>{description}</p>\
             {preview}\
             <div class=\"card qr\">{qr}</div>\
             <p><a href=\"{uri}\">Open</a></p><code>{uri}</code>",
            name = listing.name,
            description = listing.description,
            qr = qr(&listing.uri),
        ),
    ))
}

pub fn router(listings: Vec<Listing>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/{slug}", get(service))
        .with_state(Arc::new(listings))
}
