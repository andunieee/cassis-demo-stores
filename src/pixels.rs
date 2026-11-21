//! The pixel wall: a small image everyone paints on. A payer picks pixels
//! by column and row and a color, then pays [`PIXEL_SATS`] for each one.

use crate::cop::{request_payment, Flow, Out, Service};
use async_trait::async_trait;
use base64::Engine;
use hypereffect::vtree::{self, button, card, column, image, input, list, notice, row, select, text};
use hypereffect::{json, Action, Handler};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::info;

const WIDTH: usize = 30;
const HEIGHT: usize = 20;
const PIXEL_SATS: u64 = 50;
/// Side of a painted pixel in the PNG sent to clients.
const SCALE: usize = 10;
const BLANK: [u8; 3] = [0xee, 0xee, 0xee];
const PALETTE: &[(&str, &str)] = &[
    ("Black", "#000000"),
    ("White", "#ffffff"),
    ("Grey", "#808080"),
    ("Red", "#e53935"),
    ("Orange", "#fb8c00"),
    ("Yellow", "#fdd835"),
    ("Green", "#43a047"),
    ("Teal", "#00897b"),
    ("Blue", "#1e88e5"),
    ("Indigo", "#3949ab"),
    ("Purple", "#8e24aa"),
    ("Pink", "#ec407a"),
    ("Brown", "#6d4c41"),
];

#[derive(Serialize, Deserialize)]
struct Canvas {
    pixels: Vec<[u8; 3]>,
}

impl Default for Canvas {
    fn default() -> Self {
        Self {
            pixels: vec![BLANK; WIDTH * HEIGHT],
        }
    }
}

impl Canvas {
    /// The canvas as a PNG data URI, every pixel drawn `SCALE` times bigger.
    fn data_uri(&self) -> String {
        let (width, height) = (WIDTH * SCALE, HEIGHT * SCALE);
        let mut rgb = Vec::with_capacity(width * height * 3);
        for y in 0..height {
            for x in 0..width {
                rgb.extend(self.pixels[(y / SCALE) * WIDTH + x / SCALE]);
            }
        }
        let mut png = Vec::new();
        let mut encoder = png::Encoder::new(&mut png, width as u32, height as u32);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        encoder
            .write_header()
            .and_then(|mut writer| writer.write_image_data(&rgb))
            .expect("encode a PNG into memory");
        format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(png)
        )
    }
}

#[derive(Clone, PartialEq)]
struct Pixel {
    col: usize,
    row: usize,
    color: [u8; 3],
}

/// What a payer has picked and not yet paid for.
#[derive(Default)]
struct Cart {
    pixels: Vec<Pixel>,
    error: Option<String>,
    paid: Option<String>,
}

pub struct Pixels {
    node: cassis_wallet::Node,
    path: PathBuf,
    canvas: Mutex<Canvas>,
    changed: broadcast::Sender<()>,
    /// Carts by session, so a reopened flow finds its pixels again.
    carts: Mutex<HashMap<String, Arc<Mutex<Cart>>>>,
}

impl Pixels {
    pub fn new(node: cassis_wallet::Node, path: PathBuf) -> Result<Arc<Self>, String> {
        let canvas = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Canvas>(&bytes)
                .ok()
                .filter(|canvas| canvas.pixels.len() == WIDTH * HEIGHT)
                .ok_or_else(|| format!("{} is not a {WIDTH}x{HEIGHT} canvas", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Canvas::default(),
            Err(error) => return Err(format!("read {}: {error}", path.display())),
        };
        Ok(Arc::new(Self {
            node,
            path,
            canvas: Mutex::new(canvas),
            changed: broadcast::channel(16).0,
            carts: Default::default(),
        }))
    }

    fn paint(&self, pixels: &[Pixel]) {
        self.change(|canvas| {
            for pixel in pixels {
                canvas.pixels[(pixel.row - 1) * WIDTH + pixel.col - 1] = pixel.color;
            }
        });
    }

    /// Fires when the picture changes.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changed.subscribe()
    }

    /// Wipe the picture back to blank.
    pub fn reset(&self) {
        self.change(|canvas| *canvas = Canvas::default());
    }

    /// How many pixels are not blank.
    pub fn painted(&self) -> usize {
        let canvas = self.canvas.lock().unwrap_or_else(|e| e.into_inner());
        canvas.pixels.iter().filter(|pixel| **pixel != BLANK).count()
    }

    /// The picture as a PNG data URI.
    pub fn image(&self) -> String {
        self.canvas.lock().unwrap_or_else(|e| e.into_inner()).data_uri()
    }

    fn change(&self, change: impl FnOnce(&mut Canvas)) {
        let json = {
            let mut canvas = self.canvas.lock().unwrap_or_else(|e| e.into_inner());
            change(&mut canvas);
            serde_json::to_vec(&*canvas).expect("serialize the canvas")
        };
        let tmp = self.path.with_extension("tmp");
        if let Err(error) = std::fs::write(&tmp, json).and_then(|_| std::fs::rename(&tmp, &self.path)) {
            tracing::error!("save {}: {error}", self.path.display());
        }
        let _ = self.changed.send(());
    }
}

#[async_trait]
impl Service for Pixels {
    fn name(&self) -> &'static str {
        "Pixel wall"
    }

    fn description(&self) -> &'static str {
        "A 30x20 image painted pixel by pixel, 50 sats each"
    }

    fn preview(&self) -> String {
        format!("<img class=\"canvas\" src=\"{}\" alt=\"The pixel wall\">", self.image())
    }

    async fn open(self: Arc<Self>, session: String, out: Out) -> Result<Arc<dyn Flow>, String> {
        let cart = self
            .carts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(session)
            .or_default()
            .clone();
        let flow = Arc::new(PixelFlow {
            service: self.clone(),
            out: out.clone(),
            cart,
        });
        out.signal(json!({"paint": {"col": 1, "row": 1, "color": PALETTE[3].1, "custom": ""}}))
            .await?;
        flow.redraw().await?;
        let mut changed = self.changed.subscribe();
        let watcher = flow.clone();
        out.spawn(async move {
            // A lagged receiver missed some changes, and still wants the latest.
            while !matches!(changed.recv().await, Err(broadcast::error::RecvError::Closed)) {
                if watcher.redraw().await.is_err() {
                    return;
                }
            }
        });
        Ok(flow)
    }
}

struct PixelFlow {
    service: Arc<Pixels>,
    out: Out,
    cart: Arc<Mutex<Cart>>,
}

fn hex(color: [u8; 3]) -> String {
    format!("#{:02x}{:02x}{:02x}", color[0], color[1], color[2])
}

fn parse_color(text: &str) -> Option<[u8; 3]> {
    let digits = text.trim().trim_start_matches('#');
    if digits.len() != 6 || !digits.is_ascii() {
        return None;
    }
    let byte = |i: usize| u8::from_str_radix(&digits[i..i + 2], 16).ok();
    Some([byte(0)?, byte(2)?, byte(4)?])
}

/// A whole number within `1..=max` out of a signal or payload value.
fn position(value: Option<&hypereffect::Value>, max: usize) -> Option<usize> {
    let number = value?.as_f64()?;
    (number.fract() == 0.0 && (1.0..=max as f64).contains(&number)).then(|| number as usize)
}

impl PixelFlow {
    async fn redraw(&self) -> Result<(), String> {
        let uri = self
            .service
            .canvas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .data_uri();
        let page = {
            let cart = self.cart.lock().unwrap_or_else(|e| e.into_inner());
            page(&uri, &cart)
        };
        self.out.redraw(page).await
    }

    fn update(&self, change: impl FnOnce(&mut Cart)) {
        change(&mut self.cart.lock().unwrap_or_else(|e| e.into_inner()));
    }

    fn pick(&self, action: &Action) -> Result<Pixel, String> {
        let col = position(action.signal("paint.col"), WIDTH)
            .ok_or(format!("The column must be a number from 1 to {WIDTH}."))?;
        let row = position(action.signal("paint.row"), HEIGHT)
            .ok_or(format!("The row must be a number from 1 to {HEIGHT}."))?;
        let signal = |path| action.signal(path).and_then(|v| v.as_str()).unwrap_or_default();
        let custom = signal("paint.custom");
        let color = if custom.trim().is_empty() {
            parse_color(signal("paint.color"))
        } else {
            parse_color(custom)
        }
        .ok_or("The color must look like #rrggbb.")?;
        Ok(Pixel { col, row, color })
    }
}

#[async_trait]
impl Flow for PixelFlow {
    async fn action(&self, action: Action) -> Result<(), String> {
        match action.name.as_str() {
            "add" => {
                let picked = self.pick(&action);
                self.update(|cart| {
                    cart.paid = None;
                    match picked {
                        Ok(pixel) => {
                            cart.error = None;
                            cart.pixels
                                .retain(|p| (p.col, p.row) != (pixel.col, pixel.row));
                            cart.pixels.push(pixel);
                        }
                        Err(error) => cart.error = Some(error),
                    }
                });
            }
            "remove" => {
                let payload = action.payload.as_ref();
                let col = position(payload.map(|p| &p["col"]), WIDTH);
                let row = position(payload.map(|p| &p["row"]), HEIGHT);
                self.update(|cart| {
                    cart.pixels.retain(|p| (Some(p.col), Some(p.row)) != (col, row));
                });
            }
            "clear" => self.update(|cart| *cart = Cart::default()),
            "pay" => {
                let pixels = self.cart.lock().unwrap_or_else(|e| e.into_inner()).pixels.clone();
                if pixels.is_empty() {
                    return Ok(());
                }
                let sats = PIXEL_SATS * pixels.len() as u64;
                let memo = format!("{} pixel(s) on the pixel wall", pixels.len());
                let flow = (self.service.clone(), self.out.clone(), self.cart.clone());
                request_payment(&self.service.node, &self.out, sats, memo, async move {
                    let (service, out, cart) = flow;
                    service.paint(&pixels);
                    info!("{} pixel(s) painted", pixels.len());
                    let flow = PixelFlow { service, out, cart };
                    flow.update(|cart| {
                        cart.pixels.retain(|p| !pixels.contains(p));
                        cart.paid = Some(format!("Paid, thank you! {} pixel(s) painted.", pixels.len()));
                    });
                    let _ = flow.redraw().await;
                })
                .await?;
                return Ok(());
            }
            other => tracing::warn!("unknown action {other}"),
        }
        self.redraw().await
    }
}

fn page(canvas: &str, cart: &Cart) -> vtree::Node {
    let mut pixels = cart.pixels.clone();
    pixels.sort_by_key(|p| (p.row, p.col));
    column("page").children([
        text("title").variant("h1").text("Pixel wall"),
        text("blurb").variant("caption").text(format!(
            "{WIDTH} x {HEIGHT} pixels, {PIXEL_SATS} sats each. Everyone paints on the same image."
        )),
        image("canvas")
            .src(canvas)
            .alt("The pixel wall")
            .variant("full"),
        card("paint")
            .title("Pick a pixel")
            .children([
                row("position").children([
                    input("col")
                        .variant("number")
                        .label(format!("Column (1-{WIDTH})"))
                        .bind("paint.col"),
                    input("row")
                        .variant("number")
                        .label(format!("Row (1-{HEIGHT})"))
                        .bind("paint.row"),
                ]),
                row("colors").children([
                    select("color")
                        .label("Color")
                        .bind("paint.color")
                        .options(PALETTE.iter().map(|(name, value)| json!({"value": value, "label": name})).collect::<Vec<_>>()),
                    input("custom")
                        .label("Or any color, as #rrggbb")
                        .placeholder("#ff00ff")
                        .bind("paint.custom"),
                ]),
            ])
            .children(cart.error.as_ref().map(|error| notice("error").variant("failure").text(error.as_str())))
            .child(
                row("paint-actions").justify("end").child(
                    button("add").label("Add to my pixels").on_press(Handler::action("add")),
                ),
            ),
        card("cart")
            .title(format!("My pixels ({})", pixels.len()))
            .child(
                list("cart-list").empty("No pixels picked yet.").children(pixels.iter().map(|p| {
                    row(format!("pixel-{}-{}", p.col, p.row)).justify("between").children([
                        text(format!("pixel-{}-{}-text", p.col, p.row)).text(format!(
                            "column {}, row {}: {}",
                            p.col,
                            p.row,
                            hex(p.color)
                        )),
                        button(format!("pixel-{}-{}-remove", p.col, p.row))
                            .variant("plain")
                            .label("Remove")
                            .on_press(Handler::action("remove").payload(json!({"col": p.col, "row": p.row}))),
                    ])
                })),
            )
            .children(cart.paid.as_ref().map(|paid| notice("paid").variant("success").text(paid.as_str())))
            .child(row("buttons").justify("end").children([
                button("clear")
                    .variant("plain")
                    .label("Clear")
                    .disabled(pixels.is_empty())
                    .on_press(Handler::action("clear")),
                button("pay")
                    .variant("primary")
                    .label(format!("Pay {} sats", PIXEL_SATS * pixels.len() as u64))
                    .disabled(pixels.is_empty())
                    .on_press(Handler::action("pay")),
            ])),
    ])
}
