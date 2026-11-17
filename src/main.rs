//! `cassis-demo-stores`: a server that offers Cassis Order Protocol
//! services, one iroh key each, and a web page that links to them. This
//! node, a Cassis wallet home, is the payee of all of them.

mod cop;
mod pixels;
mod wall;
mod web;

use cassis_wallet::{Home, Node, NodeConfig};
use clap::Parser;
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, info_span};

#[derive(Parser)]
#[command(about = "Cassis Order Protocol demo stores")]
struct Cli {
    /// Wallet home directory, with the networks to be paid on enabled
    /// (see `cassis-wallet enable`). Defaults to `$CASSIS_HOME`, else
    /// `~/.cassis`.
    #[arg(long, value_name = "DIR")]
    home: Option<PathBuf>,
    /// Where the stores keep what they sell.
    #[arg(long, value_name = "DIR", default_value = "data")]
    data: PathBuf,
    /// Where to serve the web page.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

/// The secret of the endpoint of the service `slug`: stable, so its URI
/// keeps working, and its own, so no two services share a key.
fn service_secret(home: &Home, slug: &str) -> [u8; 32] {
    Sha256::new()
        .chain_update(b"cassis-demo-stores/")
        .chain_update(slug)
        .chain_update(home.keys.order_endpoint_secret())
        .finalize()
        .into()
}

#[tokio::main]
async fn main() {
    cassis_core::logging::init_logging();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let home = Home::open(cli.home)?;
    let span = info_span!("node", node = "demo-stores");
    let networks = home
        .open_networks(&home.config.network_ids()?, &span)
        .await?;
    if networks.is_empty() {
        return Err("no networks enabled; see `cassis-wallet enable`".into());
    }
    let node = Node::start(
        NodeConfig {
            keys: home.keys.clone(),
            db: home.db.clone(),
            span,
            serve: true,
        },
        networks,
    )
    .await?;
    std::fs::create_dir_all(&cli.data).map_err(|e| format!("create {}: {e}", cli.data.display()))?;

    let services: [(&str, Arc<dyn cop::Service>); 2] = [
        (
            "pixels",
            pixels::Pixels::new(node.clone(), cli.data.join("pixels.json"))?,
        ),
        (
            "wall",
            wall::Wall::new(node.clone(), cli.data.join("wall.json"))?,
        ),
    ];
    let mut listings = Vec::new();
    let mut endpoints = Vec::new();
    for (slug, service) in services {
        let endpoint = cop::bind(service_secret(&home, slug)).await?;
        let uri = cop::uri(&endpoint, service.name());
        println!("{}: {uri}", service.name());
        listings.push(web::Listing {
            slug,
            name: service.name(),
            description: service.description(),
            uri,
        });
        endpoints.push(tokio::spawn(cop::serve(endpoint, service)));
    }

    let listener = tokio::net::TcpListener::bind(cli.listen)
        .await
        .map_err(|e| format!("listen on {}: {e}", cli.listen))?;
    info!("web page on http://{}; Ctrl-C to stop", cli.listen);
    axum::serve(listener, web::router(listings))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| format!("serve the web page: {e}"))?;
    for endpoint in endpoints {
        endpoint.abort();
    }
    node.shutdown().await;
    Ok(())
}
