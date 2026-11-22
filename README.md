# cassis-demo-stores

Demo storefronts for the [Cassis Order Protocol](https://github.com/andunieee/cassis):
a pixel board, a wall of numbers, and a faucet, each sold over its own iroh
endpoint, plus a web page that links to them.

## Run

```sh
cargo run --release
```

Then open <http://127.0.0.1:8080> and add a network in the admin interface.
The wallet home defaults to `./cassis_wallet` and store data to `./data`; both
are gitignored.
