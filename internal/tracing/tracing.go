package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// InitTracer инициализирует OpenTelemetry трейсер
func InitTracer(ctx context.Context, serviceName, jaegerEndpoint string) (*sdktrace.TracerProvider, error) {
	// Создаём экспортёр — отправляет трейсы в Jaeger
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(stripProtocol(jaegerEndpoint)),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create exporter: %w", err)
	}

	// Ресурс — описание нашего сервиса
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	// Создаём TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Устанавливаем глобальный TracerProvider
	otel.SetTracerProvider(tp)

	return tp, nil
}

// GetTracer возвращает трейсер для пакета
func GetTracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// stripProtocol убирает http:// из адреса
func stripProtocol(endpoint string) string {
	if len(endpoint) > 7 && endpoint[:7] == "http://" {
		return endpoint[7:]
	}
	if len(endpoint) > 8 && endpoint[:8] == "https://" {
		return endpoint[8:]
	}
	return endpoint
}
