package logger

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
)

var (
	logger   *zap.Logger
	logLevel = zap.NewAtomicLevel()
)

func NewLogger(serviceName string) *zap.Logger {
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		panic(err)
	}

	logFile := filepath.Join(logDir, serviceName+".log")

	//file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err != nil {
	//	panic(err)
	//}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.LevelKey = "level"
	encoderConfig.MessageKey = "msg"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)

	// 使用lumberjack进行日志轮转
	var writer io.Writer
	writer = &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    500,  // megabytes
		MaxBackups: 7,    // 保留的旧文件数
		MaxAge:     7,    // days
		Compress:   true, // 是否压缩
	}

	//fileCore := zapcore.NewCore(jsonEncoder, zapcore.AddSync(file), logLevel)
	fileCore := zapcore.NewCore(jsonEncoder, zapcore.AddSync(writer), logLevel)
	consoleCore := zapcore.NewCore(zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), zapcore.Lock(os.Stdout), zap.InfoLevel)

	core := zapcore.NewTee(fileCore, consoleCore)
	//core := zapcore.NewTee(fileCore)

	logger = zap.New(core, zap.AddCaller())
	return logger
}

func SetLogLevel(level string) {
	zapLevel, err := zapcore.ParseLevel(level)
	if err != nil {
		return
	}
	logLevel.SetLevel(zapLevel)
	logger.Info("Log level set to", zap.String("level", level))
}

func WithTrace(ctx context.Context, logger *zap.Logger) *zap.Logger {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	return logger.With(
		zap.String("trace_id", sc.TraceID().String()),
		zap.String("span_id", sc.SpanID().String()),
	)
}

func NewLoggerWithTrace(ctx context.Context, logger *zap.Logger) *zap.Logger {
	if span := SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		return logger.With(
			zap.String("trace_id", sc.TraceID().String()),
			zap.String("span_id", sc.SpanID().String()),
		)
	}
	return logger
}
