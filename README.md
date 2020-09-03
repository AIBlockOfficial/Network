# Zenotta Network Protocol

A repo for the development of the Zenotta Network Protocol (ZNP).

..

## Setup

Run the following when first cloning the repo in order to get setup and ready to go:

```
make
```

Once done you can build the project using `cargo build` and run it using `cargo run`, as per usual.

..

## Running example nodes

To run a test compute node:

```
cargo run --bin compute
```

You can optionally supply IP address & port arguments as `--ip 127.0.0.1 --port 12345`. You can learn more about available options by running `cargo run --bin compute -- --help`.

Similarly, you can run a miner node and provide a compute node address:

```
cargo run --bin miner -- --connect 127.0.0.1:12345
```

To enable log output, set this environment variable:

```
RUST_LOG=system=trace
```

## Documentation

Documentation can be built locally with rustdoc by running the following command:

```
cargo +nightly doc --document-private-items
```

The resulting documentation can be found in `target/doc/system/index.html`.

Nightly is required because one of dependencies (`gmp-mpfr-sys`) uses unstable features.
