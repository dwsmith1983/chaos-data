package local

import (
	"context"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

var _ adapter.EventReader = (*NoopEventReader)(nil)

type NoopEventReader struct{}

func NewNoopEventReader() *NoopEventReader {
	return &NoopEventReader{}
}

func (n *NoopEventReader) Manifest(_ context.Context) ([]types.ChaosEvent, error) {
	return nil, nil
}
