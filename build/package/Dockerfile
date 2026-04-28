############################
# Build stage: compile the Go application
# Uses golang:1.26-alpine as the base builder image
# https://hub.docker.com/_/golang
############################
FROM golang:1.26-alpine AS builder

# Install build dependencies (ca-certificates for HTTPS, git for VCS, openssh-client for SSH, upx for compression)
RUN apk --no-cache add ca-certificates git openssh-client upx

# Set working directory
WORKDIR /build

# Build-time arguments for version injection (can be overridden at build time)
ARG VERSION="dev"
ARG GIT_COMMIT="unknown"
ARG BUILD_DATE="unknown"

# Copy go.mod and go.sum files
# These are copied separately to leverage Docker layer caching
# If source code changes but dependencies don't, the layer will be reused
COPY go.mod go.sum ./

# Download Go module dependencies
# Mounts the host ssh-agent socket so the SSH key is never copied into the image
# Rewrites HTTPS GitHub URLs to SSH so `go mod download` uses the forwarded agent for private modules
RUN --mount=type=ssh \
    mkdir -p -m 0700 ~/.ssh && \
    ssh-keyscan github.com >> ~/.ssh/known_hosts && \
    git config --global url."git@github.com:".insteadOf "https://github.com/" && \
    go mod download

# Copy application source code
COPY ./cmd ./cmd
COPY ./internal ./internal

# Build the binary with optimized flags
# GOARCH=amd64 GOOS=linux: target Linux x86-64 architecture
# CGO_ENABLED=0: static binary with no C dependencies (works with distroless)
# -trimpath: removes absolute file paths from binary for reproducibility and smaller size
# -ldflags:
#   -w -s: strip debug symbols and DWARF information
#   -X: inject version, commit, and build date at runtime
RUN GOARCH=amd64 GOOS=linux CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-w -s" \
    -o app ./cmd

# Compress the binary using upx to further reduce image size
RUN upx -q app

############################
# Runtime stage: minimal production image
# Uses distroless for the smallest possible image with only runtime dependencies
# https://github.com/GoogleContainerTools/distroless
############################
FROM gcr.io/distroless/static-debian13:nonroot

# Set the default port (can be overridden at runtime with -e PORT=...)
ENV PORT=8080

# Go runtime tuning — all overridable at docker run / compose environment.
# GOGC=200: GC fires when live heap doubles (default 100); fewer pause cycles.
# GOMEMLIMIT: hard cap ensures GC still triggers before OOM under high burst.
ENV GOGC="200"
ENV GOMEMLIMIT="256MiB"

# Copy the compiled binary from the builder stage
# Uses absolute path /go/bin/app to follow distroless conventions
# The binary is already stripped and compressed, so the final image is minimal
COPY --from=builder /build/app /go/bin/app

# Expose the default port
EXPOSE $PORT

# Run as the nonroot user (distroless default, provides security isolation)
USER nonroot:nonroot

# Container entrypoint
ENTRYPOINT ["/go/bin/app"]
