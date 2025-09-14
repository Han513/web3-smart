package logger

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk_trace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

func InitTrace(serviceNamespace, serviceName string) {
	traceProvider := sdk_trace.NewTracerProvider(
		sdk_trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNamespaceKey.String(serviceNamespace),
			semconv.ServiceNameKey.String(serviceName),
		)),
	)
	otel.SetTracerProvider(traceProvider)
}

func StartSpan(ctx context.Context, tracerName, spanName string) (context.Context, trace.Span) {
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName)
	return ctx, span
}

func StartSpanWithRequest(r *http.Request, tracerName, spanName string) (context.Context, trace.Span) {
	ctx, span := otel.Tracer(tracerName).Start(GetContextWithRequest(r), spanName)
	if r != nil {
		span.SetAttributes(
			semconv.HTTPMethodKey.String(r.Method),
			semconv.HTTPURLKey.String(r.URL.Path),
		)
	}
	return ctx, span
}

func GetContextWithRequest(r *http.Request) context.Context {
	return otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
}

func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}
