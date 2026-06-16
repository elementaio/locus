# Build a small, static Locus image. Alpine's default target is musl, so the
# release binary is statically linked and runs on `scratch` with nothing else.
FROM rust:1-alpine AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY . .
# rust-toolchain.toml pins the exact compiler; rustup installs it on first use.
RUN cargo build --release && strip target/release/locus

FROM scratch
COPY --from=build /src/target/release/locus /locus
# RESP port. Override host/port/persistence via env (see README).
ENV LOCUS_PORT=6379
EXPOSE 6379
# Default RDB path is ./locus.rdb (CWD = /). Mount a volume and set LOCUS_RDB
# for persistence across restarts, e.g. -v locus-data:/data -e LOCUS_RDB=/data/locus.rdb
ENTRYPOINT ["/locus"]
