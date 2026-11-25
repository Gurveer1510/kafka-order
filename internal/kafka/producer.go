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

	results := client.ProduceSync(ctx, rcrd)
	if results.FirstErr() != nil {
		return results.FirstErr()
	}
	return nil
}
