FROM --platform=$BUILDPLATFORM rust:1.79.0-slim-bullseye AS chef

RUN apt-get update && apt-get -y --no-install-recommends install git build-essential m4 llvm libclang-dev diffutils curl cmake libglfw3-dev libxrandr-dev libxinerama-dev libxcursor-dev libxi-dev python3
RUN cargo install cargo-chef 
WORKDIR /aiblock
ENV CARGO_TARGET_DIR=/aiblock

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /aiblock/recipe.json /aiblock/recipe.json 

ARG TARGETPLATFORM
ARG BUILDPLATFORM

RUN cargo chef cook --release --recipe-path /aiblock/recipe.json
COPY . .
RUN cargo build --release

# Use distroless
#FROM cgr.dev/chainguard/glibc-dynamic:latest
#
FROM rust:1.79.0-slim-bullseye
RUN apt-get update && apt-get -y --no-install-recommends install libclang-dev libxinerama-dev
#USER nonroot

# Set these in the environment to override [use once we have env vars available]
ARG NODE_TYPE_ARG="mempool"
ENV NODE_TYPE=$NODE_TYPE_ARG
ENV CONFIG="/etc/node_settings.toml"
ENV TLS_CONFIG="/etc/tls_certificates.json"
ENV INITIAL_BLOCK_CONFIG="/etc/initial_block.json"
ENV API_CONFIG="/etc/api_config.json"
ENV INITIAL_ISSUANCE="/etc/initial_issuance.json"
ENV API_USE_TLS="0"
ENV MEMPOOL_MINER_WHITELIST="/etc/mempool_miner_whitelist.json"
ENV RUST_LOG=info,debug

# Copy node bin
COPY --from=builder /aiblock/release/node /aiblock/aiblock
#COPY --from=builder /usr/lib/x86_64-linux-gnu/libX11.so.6 /usr/lib/x86_64-linux-gnu/libX11.so.6
#RUN cp  /aiblock/release/node /aiblock/aiblock

# Default config for the node
COPY .docker/conf/* /etc/.

ENTRYPOINT ["/aiblock/aiblock"]

CMD [$NODE_TYPE]


