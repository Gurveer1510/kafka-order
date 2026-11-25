package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func PublishEvent(ctx context.Context, topic string, client *kgo.Client, key, value []byte) error {

	rcrd := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}
	return client.ProduceSync(ctx, rcrd).FirstErr()
}
