FROM rust:1-slim AS build
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:stable-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/fishtank_monitor /usr/local/bin/
EXPOSE 8080
CMD ["fishtank_monitor", "/etc/fishtank/config.toml"]
