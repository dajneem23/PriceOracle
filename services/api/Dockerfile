# Build stage
FROM rust:1.83-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY rustfmt.toml clippy.toml deny.toml ./

# Copy source code
COPY src ./src
COPY crates ./crates

# Build the application
RUN cargo build --release -p api

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/api /app/api

# Copy configuration
COPY crates/api/config.toml /app/config.toml

# Expose the port
EXPOSE 8082

# Run the binary
CMD ["/app/api"]
