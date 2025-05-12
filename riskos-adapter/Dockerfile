FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/riskos-adapter ./cmd/adapter

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/riskos-adapter /riskos-adapter
USER nonroot:nonroot
ENTRYPOINT ["/riskos-adapter"]
