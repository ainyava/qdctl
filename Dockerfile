FROM rust:1.87-slim AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl unzip tar && \
    curl -fsSL https://downloads.rclone.org/rclone-current-linux-amd64.zip -o /tmp/rclone.zip && \
    unzip /tmp/rclone.zip -d /tmp && \
    mv /tmp/rclone-*-linux-amd64/rclone /usr/local/bin/rclone && \
    rm -rf /tmp/rclone.zip /tmp/rclone-* && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/qdctl /usr/local/bin/qdctl
COPY scripts/*.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/*.sh
