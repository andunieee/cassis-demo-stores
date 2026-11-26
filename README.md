# cassis-demo-stores

Demo storefronts for the [Cassis Order Protocol](https://github.com/andunieee/cassis):
a pixel board, a wall of numbers, a faucet, and a casino, all served over the
node's own iroh endpoint — the same key that is the payee of every invoice —
plus a web page that links to them. Each store is told apart by the `store`
parameter in its `cop://` URI.

## Run

```sh
cargo run --release
```

Then open <http://127.0.0.1:8080> and add a network in the admin interface.
The wallet home defaults to `./cassis_wallet` and store data to `./data`; both
are gitignored.

## Admin access

The admin dashboard is not part of the HTTP page. It is its own iroh endpoint,
printed at startup as `hyperb://<key>/admin`, and opened in a hyperb client.
The key in that URL identifies the server's admin endpoint; it is not a
credential. Who may use the dashboard is decided by the list of iroh keys in
`data/operators.json`, an array of lowercase 64-character hex keys. A client
connects with its own iroh key, and only a key in that file gets a session —
unless it is pairing itself with the startup link.

- At every start the server prints a one-time pairing link
  `hyperb://<id>/admin#setup=<secret>`, whether or not operators are already
  paired. The first key to open it is written to `operators.json` and becomes
  an operator; the secret is then discarded. Restart to print a new one.
- An operator can add or remove other keys from the **Operators** tab. An
  operator cannot remove their own key, but can remove any other, even the
  last one.
- To add a key without the dashboard, append it to `operators.json` and
  restart the server. The file is read at startup and is only saved by the
  dashboard, not watched for edits.

Anyone whose key is in `operators.json` can move the wallets' funds, so treat
that file, the admin endpoint and the wallet home as secrets.

## Release binary

`.github/workflows/release.yml` builds a fully static
`x86_64-unknown-linux-musl` binary, so it runs on a VPS whatever its distro or
glibc version. Push a `v*` tag to publish a GitHub release, or run the workflow
manually and download the `cassis-demo-stores-x86_64-unknown-linux-musl`
artifact.

The build enables the `bitcoin` and `lightning` network backends by default.
To ship others, pass a comma-separated list as the workflow's `features` input,
for example `bitcoin,lightning,cashu`. Keep `vendored-openssl` in the list when
building `bitcoin`: its esplora client uses native-tls, and vendoring OpenSSL is
what keeps the binary static.

A release contains the raw binary, a tarball with the binary and this README,
and `SHA256SUMS`. The binary is self-contained; the VPS only needs writable
directories for `--home` and `--data`, plus the usual CA certificates for
outgoing TLS. On the VPS:

```sh
tar xzf cassis-demo-stores-*-x86_64-unknown-linux-musl.tar.gz
sudo install -m 755 cassis-demo-stores/cassis-demo-stores /usr/local/bin/
cassis-demo-stores --home /var/lib/cassis --data /var/lib/cassis/data \
  --listen 127.0.0.1:8080
```
