package job

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// JobFunc 定义作业执行函数
type JobFunc func(ctx context.Context) error

// Scheduler 作业调度器
type Scheduler struct {
	jobs    map[string]*ScheduledJob
	running bool
	mu      sync.Mutex
	logger  *zap.Logger
}

// ScheduledJob 表示一个调度的作业
type ScheduledJob struct {
	name     string
	interval time.Duration
	fn       JobFunc
	stopCh   chan struct{}
	done     sync.WaitGroup
	cancel   context.CancelFunc
	once     bool
}

// NewScheduler 创建调度器
func NewScheduler(logger *zap.Logger) *Scheduler {
	return &Scheduler{
		jobs:   make(map[string]*ScheduledJob),
		logger: logger,
	}
}

// RegisterJob 注册作业
func (s *Scheduler) RegisterJob(name string, interval time.Duration, fn JobFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[name] = &ScheduledJob{
		name:     name,
		interval: interval,
		fn:       fn,
		stopCh:   make(chan struct{}),
		once:     false,
	}

	s.logger.Info("Registered job", zap.String("job", name), zap.Duration("interval", interval))
}

// RegisterOnceJob RegisterJob 注册只运行一次的作业
func (s *Scheduler) RegisterOnceJob(name string, fn JobFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[name] = &ScheduledJob{
		name:     name,
		interval: 0,
		fn:       fn,
		stopCh:   make(chan struct{}),
		once:     true,
	}

	s.logger.Info("Registered once job", zap.String("job", name))
}

// Start 启动调度器
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return
	}

	s.running = true

	for _, job := range s.jobs {
		// 对每个作业创建副本以避免闭包陷阱
		j := job
		j.done.Add(1)

		go func() {
			defer j.done.Done()
			if j.once {
				s.runOnceJob(ctx, j)
			} else {
				s.runJob(ctx, j)
			}
		}()
	}
}

// Stop 停止调度器
func (s *Scheduler) Stop(ctx context.Context) {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false

	// 关闭所有作业的停止通道
	for _, job := range s.jobs {
		if job.cancel != nil {
			job.cancel() // 调用 cancel 来提前终止任务
		}
		close(job.stopCh)
	}
	s.mu.Unlock()

	s.logger.Warn("Stopping scheduler...")

	// 等待所有作业完成
	wg := &sync.WaitGroup{}
	for _, job := range s.jobs {
		wg.Add(1)
		go func(j *ScheduledJob) {
			defer wg.Done()
			waitCh := make(chan struct{})
			go func() {
				j.done.Wait()
				close(waitCh)
			}()

			select {
			case <-waitCh:
				return
			case <-ctx.Done():
				s.logger.Warn("Context deadline exceeded while waiting for job to stop",
					zap.String("job", j.name))
				return
			}
		}(job)
	}

	// 等待所有作业或超时
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		s.logger.Info("All jobs stopped successfully")
	case <-ctx.Done():
		s.logger.Warn("Context deadline exceeded while waiting for jobs to stop")
	}
}

// runOnceJob 运行单次任务
func (s *Scheduler) runOnceJob(ctx context.Context, job *ScheduledJob) {
	s.logger.Info("Running one-time job", zap.String("job", job.name))
	s.executeJob(ctx, job)
}

// runJob 运行单个作业
func (s *Scheduler) runJob(ctx context.Context, job *ScheduledJob) {
	s.logger.Info("Running job", zap.String("job", job.name), zap.Bool("once", job.once))

	ticker := time.NewTicker(job.interval)
	defer ticker.Stop()

	// 立即运行一次
	s.executeJob(ctx, job)

	for {
		select {
		case <-ticker.C:
			s.executeJob(ctx, job)
		case <-job.stopCh:
			s.logger.Info("Stopping job", zap.String("job", job.name))
			return
		case <-ctx.Done():
			s.logger.Info("Context cancelled, stopping job", zap.String("job", job.name))
			return
		}
	}
}

// executeJob 执行作业并处理错误
func (s *Scheduler) executeJob(ctx context.Context, job *ScheduledJob) {
	jobCtx, cancel := context.WithCancel(ctx)
	if !job.once {
		// 周期任务增加超时时间
		jobCtx, cancel = context.WithTimeout(ctx, job.interval/2)
	}
	job.cancel = cancel
	defer cancel()

	s.logger.Debug("Starting job execution", zap.String("job", job.name))
	startTime := time.Now()

	if err := job.fn(jobCtx); err != nil {
		s.logger.Error("Job execution failed",
			zap.String("job", job.name),
			zap.Error(err),
			zap.Duration("duration", time.Since(startTime)))
	} else {
		s.logger.Debug("Job execution completed",
			zap.String("job", job.name),
			zap.Duration("duration", time.Since(startTime)))
	}
}
