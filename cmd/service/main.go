package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"wb-order-service/internal/cache"
	"wb-order-service/internal/config"
	"wb-order-service/internal/handler"
	kafkaconsumer "wb-order-service/internal/kafka"
	"wb-order-service/internal/metrics"
	"wb-order-service/internal/repository"
	"wb-order-service/internal/service"
	"wb-order-service/internal/tracing"

	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Service stopped with error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Загружаем .env файл
	_ = godotenv.Load()

	log.Println("Starting Order Service...")

	// 1. Загружаем конфигурацию из переменных окружения
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	log.Println("Config loaded from environment")

	// Трейсинг
	ctx := context.Background()
	tp, err := tracing.InitTracer(ctx, cfg.Tracing.ServiceName, cfg.Tracing.JaegerEndpoint)
	if err != nil {
		log.Printf("Warning: could not init tracer: %v\n", err)
	} else {
		defer func() {
			// Свой таймаут: если Jaeger недоступен, отправка последних трейсов не повесит остановку
			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := tp.Shutdown(tctx); err != nil {
				log.Printf("Error shutting down tracer: %v\n", err)
			}
		}()
		log.Println("Tracer initialized (Jaeger)")
	}

	//2. Подключение к БД Postgres
	repo, err := repository.NewPostgresRepository(ctx, cfg.Postgres.ConnString(), cfg.Postgres.Timeout)
	if err != nil {
		return fmt.Errorf("connect to DB: %w", err)
	}
	defer repo.Close()
	log.Println("Connected to PostgreSQL")

	// 3.Создаем кэш c TTL
	orderCache := cache.NewOrderCache(cfg.Cache.TTL, cfg.Cache.MaxSize)
	defer orderCache.Close()                       // останавливаем фоновую очистку кэша
	metrics.RegisterOrdersInCache(orderCache.Size) // orders_in_cache считается в момент сбора метрик

	// 4. Создаем сервисный слой
	orderService := service.NewOrderService(repo, orderCache)

	// 5. Восстанавливаем кэш из БД
	if err := orderService.RestoreCache(ctx, cfg.Cache.RestoreLimit); err != nil {
		log.Printf("Warning: could not restore cache from DB: %v\n", err)
	}

	//6. Запускаем Kafka Consumer в отдельной горутине
	kafkaCtx, kafkaCancel := context.WithCancel(ctx)
	defer kafkaCancel()

	consumer := kafkaconsumer.NewConsumer(cfg.Kafka, orderService)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v\n", err)
		}
	}()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		consumer.Run(kafkaCtx)
	}()
	log.Println("Kafka consumer started")

	//7. Настраиваем HTTP-сервер
	h := handler.NewHandler(orderService)
	router := h.NewRouter()

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      router,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("HTTP server started on http://localhost:%d\n", cfg.HTTP.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// 9. Graceful shutdown - ждем сигнал завершения или падение HTTP-сервера
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	var runErr error
	select {
	case sig := <-quit:
		log.Printf("Received signal: %v. Shutting down...\n", sig)
	case err := <-serverErr:
		log.Printf("HTTP server error: %v. Shutting down...\n", err)
		runErr = fmt.Errorf("http server: %w", err)
	}

	// 10. Остановка
	kafkaCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, cfg.HTTP.WriteTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v\n", err)
	}

	select {
	case <-consumerDone:
		log.Println("Kafka consumer finished")
	case <-shutdownCtx.Done():
		log.Println("Timeout waiting for Kafka consumer")
	}

	log.Println("Service stopped gracefully!")
	return runErr
}
