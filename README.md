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
