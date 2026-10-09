# syntax=docker/dockerfile:1
FROM rust:1-slim AS build
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Cache mounts keep downloaded crates and compiled dependencies between builds;
# the binary is copied out because the mounted target/ is not part of the image
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/fishtank_monitor /fishtank_monitor

FROM debian:stable-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /fishtank_monitor /usr/local/bin/
EXPOSE 8080
CMD ["fishtank_monitor", "/etc/fishtank/config.toml"]
