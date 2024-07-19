<div align="center">
  <a>
    <img src="https://github.com/AIBlockOfficial/Network/blob/develop/assets/hero.jpg" alt="Logo" style="width:100%;max-width:700px">
  </a>

  <h2 align="center">AIBlock Network</h2> <div style="height:30px"></div>
<!-- 
  <div>
  <img src="https://img.shields.io/github/actions/workflow/status/AIBlockOfficial/Chain/.github/workflows/rust.yml?branch=main" alt="Pipeline Status" style="display:inline-block"/>
  <img src="https://img.shields.io/crates/v/tw_chain" alt="Cargo Crates Version" style="display:inline-block" />
  </div> -->

  <p align="center">
    The network that runs the AIBlock chain.
    <br />
    <br />
    <a href="https://aiblock.dev"><strong>Official documentation »</strong></a>
    <br />
    <br />
  </p>
</div>

..

## Setup

The AIBlock Network runs on Rust, so installing this is the first step before dealing with any code. You can install `rustup`, Rust's toolchain installer, by running the following:

```
curl https://sh.rustup.rs -sSf | sh
```

When asked how to proceed, simply selecting the option `1) Proceed with installation` is generally the best. You can then run the following to update the `PATH` variable and check whether everything installed correctly:

```
source $HOME/.cargo/env
rustc --version
```

If the terminal responds with the `rustc` version you're currently running then everything went well, and you're ready to go. 

### Linux

Linux (Ubuntu 20.04.01 LTS) may require extra package installations depending on what you've developed before. The following package installs assume a completely new machine instance, and should cover everything you need to get going:

```
sudo apt install build-essential
sudo apt-get install m4
sudo apt-get install llvm
sudo apt-get install libclang-dev
sudo apt-get install clang
```

The above should enable you to install `librocksdb-sys` successfully, but older versions of this crate had bugs so it would be wise to ensure you've installed `rocksdb = "0.21.0"` or higher in order to avoid compilation issues.

..

## Running Nodes Locally

You can build everything by running

```rust
cargo build --release
```

This will compile everything into a release state, from which you can then run your nodes locally. The following are example commands for each type to get you up and running quickly:

- **Mempool**: `RUST_LOG=warp target/release/node mempool --config=src/bin/node_settings_local_raft_1.toml`
- **Storage**: `RUST_LOG=warp target/release/node storage --config=src/bin/node_settings_local_raft_1.toml`
- **Miner**: `RUST_LOG=warp target/release/node miner --config=src/bin/node_settings_local_raft_1.toml`
- **User**: `RUST_LOG=warp target/release/node user --config=src/bin/node_settings_local_raft_1.toml`

You can provide a number of flags to the command depending on the type of node, and you can view information on the available flags for each node type by running the compiled binary with the `--help` flag (e.g. `target/release/storage --help`). You can also run a full, 1 node system in your local environment by running `sh src/bin/node_settings_local_raft_1_run.sh` and perusing the generated logs. 

If you run into TLS problems on the API routes, you can pass `--api_use_tls=0` to the shell script in order to disable TLS. **Note that this will create a security concern**, so it's best not to use this too frequently or for anything public facing.

..


## Git Flow

**When working on this repo, please ensure that any branches you may create pull from `develop` regularly. In doing this you 
ensure that your local version has the latest code for the project and minimizes the possibility of unnecessary merge 
conflicts.**

AIBlock's Git flow generally involves working on each new task in a new branch, which you should checkout from `develop` and can be done as in the following example

```
git checkout -b branch_name
```

where `branch_name` would be replaced with your chosen branch name. There is no general branch naming convention aside from two cases:

- *New features*: These should be prefixed with `feature_` and then the branch name (e.g. `feature_new_cool_feature`)
- *Bugfixes*: These should be prefixed with `bugfix_` and then the branch name (e.g. `bugfix_new_damn_bug`)

Beyond this, it is only expected that branches have sensible naming that describes what the branch involves or is for.

..

## Deployment

Steps to deploy the node binaries in choice of your environment.

- Run the pipeline for branch of your choice with following variables.
- Set `deploy_binaries` to `true` to enforce infrastructure changes.
- Set `ai_block_env` with choice of your environment. e.g - `dev-stg`, `dev-byron` etc.
- Successful run of `infra` pipeline will present with you IP addresses of different nodes.
- IP address of node will still need to be configured in `node_settings.toml` and `tls_certificates.json` manually post rollout.

..

## Documentation

Documentation can be built locally with rustdoc by running the following command:

```
cargo +nightly doc --document-private-items
```

The resulting documentation can be found in `target/doc/system/index.html`.

Nightly is required because one of dependencies (`gmp-mpfr-sys`) uses unstable features.

## Trivy Code Scanning Exceptions

Trivy scanning will run for each PR submitted although there is a mechanism via which certain rules can be ignored:

Take the following output as an example

```
Dockerfile (dockerfile)

Tests: 27 (SUCCESSES: 25, FAILURES: 2, EXCEPTIONS: 0)
Failures: 2 (UNKNOWN: 0, LOW: 1, MEDIUM: 1, HIGH: 0, CRITICAL: 0)

MEDIUM: Specify a tag in the 'FROM' statement for image 'cgr.dev/chainguard/glibc-dynamic'
══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
When using a 'FROM' statement you should use a specific tag to avoid uncontrolled behavior when the image is updated.

See https://avd.aquasec.com/misconfig/ds001
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 Dockerfile:20
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  20 [ FROM cgr.dev/chainguard/glibc-dynamic:latest
```

Here we can see the rules is located at https://avd.aquasec.com/misconfig/ds001 and when navigating to the url --> https://avd.aquasec.com/misconfig/dockerfile/general/avd-ds-0001/

The last portion of the url can always be used as the ID i.e avd-ds-0001 --> AVD-DS-0001, so if we wanted to ignore this rule we would add the following to the .trivyignore file

**.trivyignore**

```
# Ignore misconfigurations
# https://avd.aquasec.com/misconfig/dockerfile/general/avd-ds-0001/
AVD-DS-0001
```

