# syntax=docker/dockerfile:1

FROM golang:1.22.5-bullseye AS build

# Install build dependencies and librdkafka dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    libssl-dev \
    libsasl2-dev \
    libzstd-dev \
    pkg-config \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

# Build and install librdkafka from source
ENV LIBRDKAFKA_VERSION=1.9.2
RUN wget https://github.com/edenhill/librdkafka/archive/refs/tags/v${LIBRDKAFKA_VERSION}.tar.gz && \
    tar -xzf v${LIBRDKAFKA_VERSION}.tar.gz && \
    cd librdkafka-${LIBRDKAFKA_VERSION} && \
    ./configure --prefix=/usr && \
    make && make install && \
    ldconfig

WORKDIR /go/src/fleet-telemetry-consumer

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with dynamic linking
RUN go build -tags dynamic -o /go/bin/fleet-telemetry-consumer

# Use a minimal base image
FROM debian:bullseye-slim

WORKDIR /

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl1.1 \
    libsasl2-2 \
    libzstd1 \
    liblz4-1 \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary and shared libraries
COPY --from=build /go/bin/fleet-telemetry-consumer /
COPY --from=build /usr/lib/librdkafka*.so* /usr/lib/

# Expose Prometheus metrics port
EXPOSE 2112

CMD ["/fleet-telemetry-consumer", "-config", "/etc/fleet-telemetry-consumer/config.json"]