package monitor

import "github.com/prometheus/client_golang/prometheus"

var (
	// KafkaMessagesReceived Kafka 消费相关
	KafkaMessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_received_total",
			Help: "Total number of messages received from Kafka.",
		},
		[]string{"topic"},
	)
	KafkaWorkerMessagesDispatched = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trade_consumer_worker_dispatch_count_total",
			Help: "Number of tasks assigned to each trade worker.",
		},
		[]string{"worker_id"},
	)
	KafkaWorkerMessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_worker_messages_processed_total",
			Help: "Total number of messages processed by each Trade consumer worker.",
		},
		[]string{"worker_id"},
	)
	KafkaWorkerProcessDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_worker_process_duration_seconds",
			Help:    "Time taken to process a message by each Trade consumer worker.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0},
		},
		[]string{"worker_id"},
	)

	// AsyncWriterMessagesQueued AsyncWriter 指标
	AsyncWriterMessagesQueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "async_writer_messages_queued_total",
			Help: "Total number of messages queued to async writer.",
		},
		[]string{"writer_id"},
	)
	AsyncWriterMessagesDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "async_writer_messages_dropped_total",
			Help: "Total number of messages dropped due to full queue.",
		},
		[]string{"writer_id"},
	)
	AsyncWriterBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "async_writer_batch_size",
			Help:    "Number of items in each batch submitted to the writer.",
			Buckets: []float64{10, 50, 100, 200, 500, 1000},
		},
		[]string{"writer_id"},
	)
	AsyncWriterFlushCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "async_writer_flush_count_total",
			Help: "Total number of batch flushes triggered.",
		},
		[]string{"writer_id"},
	)
	AsyncWriterFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "async_writer_flush_duration_seconds",
			Help:    "Time taken to flush a batch.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0},
		},
		[]string{"writer_id"},
	)
	AsyncWriterItemsWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "async_writer_items_written_total",
			Help: "Total number of items successfully written by the async writer.",
		},
		[]string{"writer_id"},
	)
)

func init() {
	prometheus.MustRegister(
		// kafka指标
		KafkaMessagesReceived,
		KafkaWorkerMessagesDispatched,
		KafkaWorkerMessagesProcessed,
		KafkaWorkerProcessDuration,

		// async 写入指标
		AsyncWriterMessagesQueued,
		AsyncWriterMessagesDropped,
		AsyncWriterBatchSize,
		AsyncWriterFlushCount,
		AsyncWriterFlushDuration,
		AsyncWriterItemsWritten,
	)
}
