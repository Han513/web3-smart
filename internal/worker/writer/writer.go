package writer

import (
	"context"
)

type BatchWriter[T any] interface {
	BWrite(ctx context.Context, batch []T) error
	Close() error
}
