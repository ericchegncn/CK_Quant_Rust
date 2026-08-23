FROM rust:1.88-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 ckquant
WORKDIR /app
RUN mkdir -p /app/user_data && chown -R ckquant:ckquant /app
COPY --from=builder /build/target/release/ck-quant-rust /usr/local/bin/ck-quant-rust
COPY config.example.json /app/config.example.json
USER ckquant
EXPOSE 8080
ENTRYPOINT ["ck-quant-rust"]
CMD ["serve", "--config", "/app/config.example.json"]
