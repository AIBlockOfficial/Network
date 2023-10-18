FROM rust:1.68.2-slim-bullseye as build

# Install build dependancies
RUN apt-get update && apt-get -y install git build-essential m4 llvm libclang-dev diffutils curl

WORKDIR /a-block

# Output artifact to workdir
ENV CARGO_TARGET_DIR=/a-block

COPY ./ ./.

# Build for release
RUN cargo build --release

# Use distroless
FROM cgr.dev/chainguard/static:latest

# Set these in the environment to override
ENV NODE_TYPE="compute"
ENV NODE_SETTINGS="--config=/etc/node_settings.toml"
ENV TLS_CERTIFICATES="--tls_config=/etc/tls_certificates.json"
ENV INITIAL_BLOCK_CONFIG="--initial_block_config=/etc/initial_block.json"
ENV API_CONFIG="--api_config=/etc/api_config.json"
ENV API_USE_TLS="--api_use_tls=0"
ENV RUST_LOG=info,debug

# Copy node bin
COPY --from=build /a-block/release/node ./node

# Default config for the node
COPY .docker/conf/* /etc/.

ENTRYPOINT ["node"]
CMD [NODE_TYPE, NODE_SETTINGS, TLS_CERTIFICATES, INITIAL_BLOCK_CONFIG, API_CONFIG , API_USE_TLS]

