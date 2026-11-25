package kafka

import "github.com/twmb/franz-go/pkg/kgo"

func NewKafkaClient(opts ...kgo.Opt) (*kgo.Client, error) {
	options := []kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
	}

	options = append(options, opts...)

	return kgo.NewClient(options...)
}
