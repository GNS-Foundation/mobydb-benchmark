# ── Builder ──────────────────────────────────────────────
FROM rust:1.86-slim AS builder

RUN apt-get update && apt-get install -y \
    clang \
    cmake \
    libclang-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies — copy crates first since Cargo.toml references them
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN mkdir -p src && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'fn main() {}' > src/seed.rs && \
    cargo build --release 2>/dev/null || true
RUN rm -rf src

# Build real source
COPY src/ src/
RUN touch src/main.rs && cargo build --release --bin benchmark-api

# ── Runtime ──────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/benchmark-api /usr/local/bin/benchmark-api

EXPOSE 3000

CMD ["benchmark-api"]

