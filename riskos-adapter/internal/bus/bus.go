// Package bus publishes decision payloads to the RiskOS bus.
//
// NATS is the default transport. The Publisher interface is small enough that
// alternate transports (Kafka, Pub/Sub) can be dropped in by adding a Dial
// helper that returns a different concrete type.
package bus

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
)

type Publisher interface {
	Publish(ctx context.Context, body []byte) error
	Close()
}

func Dial(url, subject string) (Publisher, error) {
	nc, err := nats.Connect(url, nats.Name("riskos-adapter"), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &natsPublisher{nc: nc, subject: subject}, nil
}

type natsPublisher struct {
	nc      *nats.Conn
	subject string
}

func (p *natsPublisher) Publish(_ context.Context, body []byte) error {
	if err := p.nc.Publish(p.subject, body); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

func (p *natsPublisher) Close() {
	if p.nc != nil {
		_ = p.nc.Drain()
	}
}
