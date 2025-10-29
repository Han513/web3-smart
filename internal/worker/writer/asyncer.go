package writer

import (
	"context"
	"sync"
	"time"
	"web3-smart/internal/worker/monitor"
	"web3-smart/pkg/utils"

	"go.uber.org/zap"
)

type AsyncBatchWriter[T any] struct {
	id            string
	workers       int
	tl            *zap.Logger
	writer        BatchWriter[T]
	inputChans    []chan T
	wg            sync.WaitGroup
	batchSize     int
	flushInterval time.Duration
}

func NewAsyncBatchWriter[T any](tl *zap.Logger, writer BatchWriter[T], batchSize int, flushInterval time.Duration, id string, workers int) *AsyncBatchWriter[T] {
	a := &AsyncBatchWriter[T]{
		id:            id,
		workers:       workers,
		tl:            tl,
		writer:        writer,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}

	a.inputChans = make([]chan T, workers)

	chanSize := 2000
	for i := 0; i < workers; i++ {
		a.inputChans[i] = make(chan T, chanSize)
	}

	return a
}

func (b *AsyncBatchWriter[T]) Start(ctx context.Context) {
	for i := 0; i < b.workers; i++ {
		b.wg.Add(1)
		workerID := i
		go b.processItems(ctx, workerID)
	}
}

func (b *AsyncBatchWriter[T]) processItems(ctx context.Context, workerID int) {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	var batch = make([]T, 0, b.batchSize)
	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				b.writeAndRecord(ctx, batch)
			}
			return
		case item, ok := <-b.inputChans[workerID]:
			if !ok {
				return
			}
			batch = append(batch, item)
			if len(batch) >= b.batchSize {
				b.writeAndRecord(ctx, batch)
				batch = make([]T, 0, b.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				b.writeAndRecord(ctx, batch)
				batch = make([]T, 0, b.batchSize)
			}
		}
	}
}

// 封装写入操作并记录指标
func (b *AsyncBatchWriter[T]) writeAndRecord(ctx context.Context, batch []T) {
	startTime := time.Now()
	size := len(batch)

	// 记录 batch size
	monitor.AsyncWriterBatchSize.WithLabelValues(b.id).Observe(float64(size))
	monitor.AsyncWriterItemsWritten.WithLabelValues(b.id).Add(float64(size))

	// 执行写入
	_ = b.writer.BWrite(ctx, batch)

	// 统计耗时
	elapsed := time.Since(startTime).Seconds()
	monitor.AsyncWriterFlushDuration.WithLabelValues(b.id).Observe(elapsed)

	// flush 次数统计
	monitor.AsyncWriterFlushCount.WithLabelValues(b.id).Inc()
}

// Submit 非阻塞
func (b *AsyncBatchWriter[T]) Submit(item T, hashKey string) {
	idx := utils.GetHashBucket(hashKey, uint32(b.workers))

	for {
		select {
		case b.inputChans[idx] <- item:
			return
		default:
			b.tl.Warn("Batch input channel submit timeout, dropping item", zap.String("id", b.id))
			return
		}
	}
}

// MustSubmit 通道满时阻塞提交
func (b *AsyncBatchWriter[T]) MustSubmit(item T, hashKey string) {
	idx := utils.GetHashBucket(hashKey, uint32(b.workers))
	b.inputChans[idx] <- item
}

func (b *AsyncBatchWriter[T]) Close() {
	for _, inputChan := range b.inputChans {
		close(inputChan)
	}
	b.wg.Wait()
	_ = b.writer.Close()
}
