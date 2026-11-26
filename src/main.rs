//! `cassis-demo-stores`: a server that offers Cassis Order Protocol
//! services over one iroh endpoint, the key of its Cassis wallet home, and
//! a web page that links to them. That same node is the payee of every
//! invoice the services issue.

mod admin;
mod casino;
mod cop;
mod faucet;
mod history;
mod pixels;
mod wall;
mod web;

use cassis_wallet::{Home, Node, NodeConfig};
use hyperb_cassis_wallet::{ConfigFile, WalletAdmin};
use clap::Parser;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, info_span, warn};

#[derive(Parser)]
#[command(about = "Cassis Order Protocol demo stores")]
struct Cli {
    /// Wallet home directory, which holds the config and the wallet data.
    /// Networks are added in the admin interface, or with `cassis-wallet
    /// enable`.
    #[arg(long, value_name = "DIR", env = "CASSIS_HOME", default_value = "./cassis_wallet")]
    home: PathBuf,
    /// Where the stores keep what they sell.
    #[arg(long, value_name = "DIR", default_value = "data")]
    data: PathBuf,
    /// Where to serve the web page.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// The most sats the faucet gives in one request.
    #[arg(long, value_name = "SATS", default_value_t = 100)]
    faucet_request: u64,
    /// The most sats the faucet gives one wallet in all.
    #[arg(long, value_name = "SATS", default_value_t = 1000)]
    faucet_wallet: u64,
}

/// The secret of the admin endpoint: stable, so its URI keeps working, and
/// its own, so it does not share the node's payee identity.
fn admin_secret(home: &Home) -> [u8; 32] {
    Sha256::new()
        .chain_update(b"cassis-demo-stores/")
        .chain_update("admin")
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
    let home = Home::open(Some(cli.home))?;
    let span = info_span!("node", node = "demo-stores");
    // The wallets are opened here and not by the node, so the admin
    // interface can move funds on the very same ones that get paid.
    let (wallets, networks) = WalletAdmin::open(
        ConfigFile::new(home.dir.join(cassis_wallet::config::FILE), "networks"),
        &home.dir,
        span.clone(),
    )
    .await?;
    if networks.is_empty() {
        warn!("no networks are open: add one in the admin interface, or see `cassis-wallet enable`");
    }
    let node = Node::start(
        NodeConfig {
            keys: home.keys.clone(),
            db: home.db.clone(),
            span,
            // The one endpoint below serves the node's payments as well as
            // the stores, so the node must not bind a second endpoint with
            // the same key.
            serve: false,
        },
        networks,
    )
    .await?;
    let settler = node.clone();
    node.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            settler.settle_once().await;
        }
    });
    let networks_changed = tokio::sync::broadcast::channel(4).0;
    wallets.attach(Arc::new(admin::NodeHost {
        node: node.clone(),
        changed: networks_changed.clone(),
    }));
    std::fs::create_dir_all(&cli.data).map_err(|e| format!("create {}: {e}", cli.data.display()))?;

    let history = history::History::open(&cli.data.join("sessions.redb"))?;
    let pixels = pixels::Pixels::new(node.clone(), cli.data.join("pixels.json"), history.clone())?;
    let wall = wall::Wall::new(node.clone(), cli.data.join("wall.json"), history.clone())?;
    let faucet = faucet::Faucet::new(
        node.clone(),
        cli.data.join("faucet.json"),
        faucet::Limits {
            request_sats: cli.faucet_request,
            wallet_sats: cli.faucet_wallet,
        },
        history.clone(),
    )?;
    let casino = casino::Casino::new(node.clone(), cli.data.join("casino.json"), history)?;
    // The pay buttons follow the networks that are open.
    let mut networks = networks_changed.subscribe();
    let stores = (pixels.clone(), wall.clone(), casino.clone());
    tokio::spawn(async move {
        while !matches!(networks.recv().await, Err(tokio::sync::broadcast::error::RecvError::Closed)) {
            stores.0.refresh();
            stores.1.refresh();
            stores.2.refresh();
        }
    });
    let services: [(&str, Arc<dyn cop::Service>); 4] = [
        ("pixels", pixels.clone()),
        ("wall", wall.clone()),
        ("faucet", faucet.clone()),
        ("casino", casino),
    ];
    // One endpoint, the node's payee identity, carries every store and the
    // node's payments. The URI's `store` parameter tells the flows apart.
    let store_endpoint = cop::bind_many(home.keys.iroh, &[cop::ALPN, cop::HOP_ALPN]).await?;
    let mut stores = HashMap::new();
    let mut listings = Vec::new();
    for (slug, service) in services {
        let uri = cop::uri(&store_endpoint, slug, service.name());
        println!("{}: {uri}", service.name());
        listings.push(web::Listing {
            slug,
            name: service.name(),
            description: service.description(),
            uri,
            service: service.clone(),
        });
        stores.insert(slug.to_string(), service);
    }
    let servers = tokio::spawn(cop::serve_stores(
        store_endpoint.clone(),
        stores,
        node.clone(),
    ));

    let admin = admin::Admin::new(&cli.data, wall, pixels, faucet, wallets)?;
    let endpoint = cop::bind(admin_secret(&home), cop::ADMIN_ALPN).await?;
    admin.announce(&endpoint);
    let admin_server = tokio::spawn(admin::serve(admin, endpoint));

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
    servers.abort();
    admin_server.abort();
    store_endpoint.close().await;
    node.shutdown().await;
    Ok(())
}
