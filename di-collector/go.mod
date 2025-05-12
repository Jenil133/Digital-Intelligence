module github.com/example/di-collector

go 1.22

require (
	github.com/redis/go-redis/v9 v9.5.1
	github.com/google/uuid v1.6.0
	github.com/prometheus/client_golang v1.19.0
	go.opentelemetry.io/otel v1.26.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.26.0
	go.opentelemetry.io/otel/sdk v1.26.0
	go.opentelemetry.io/otel/trace v1.26.0
)
