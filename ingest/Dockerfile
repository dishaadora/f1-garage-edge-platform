# --- build stage ---
FROM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
# CGO_ENABLED=0 produces a fully static binary with no C library
# dependencies, which is what makes the distroless runtime image valid.
RUN CGO_ENABLED=0 GOOS=linux go build -o /ingest .

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12
COPY --from=builder /ingest /ingest
EXPOSE 8080
ENTRYPOINT ["/ingest"]
CMD ["--session-id", "default-session", "--data-dir", "/data", "--flush-interval", "5s"]