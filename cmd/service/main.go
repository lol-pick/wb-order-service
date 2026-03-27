package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"wb-order-service/internal/cache"
	"wb-order-service/internal/config"
	"wb-order-service/internal/handler"
	kafkaconsumer "wb-order-service/internal/kafka"
	"wb-order-service/internal/repository"
	"wb-order-service/internal/service"
	"wb-order-service/internal/tracing"

	"github.com/joho/godotenv"
)

func main() {
	// Загружаем .env файл
	_ = godotenv.Load()

	log.Println("Starting Order Service...")

	// 1. Загружаем конфигурацию из переменных окружения
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v\n", err)
	}
	log.Println("Config loaded from environment")

	// Трейсинг
	ctx := context.Background()
	tp, err := tracing.InitTracer(ctx, cfg.Tracing.ServiceName, cfg.Tracing.JaegerEndpoint)
	if err != nil {
		log.Printf("Warning: could not init tracer: %v\n", err)
	} else {
		defer func() {
			if err := tp.Shutdown(ctx); err != nil {
				log.Printf("Error shutting down tracer: %v\n", err)
			}
		}()
		log.Println("Tracer initialized (Jaeger)")
	}

	//2. Подключение к БД Postgres
	repo, err := repository.NewPostgresRepository(ctx, cfg.Postgres.ConnString(), cfg.Postgres.Timeout)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v\n", err)
	}
	defer repo.Close()
	log.Println("Connected to PostgreSQL")

	// 3.Создаем кэш c TTL
	orderCache := cache.NewOrderCache(cfg.Cache.TTL)

	// 4. Создаем сервисный слой
	orderService := service.NewOrderService(repo, orderCache)

	// 5. Восстанавливаем кэш из БД
	if err := orderService.RestoreCache(ctx); err != nil {
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

	go consumer.Run(kafkaCtx)
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

	// 8. Запускаем HTTP-сервер в отдельной горутине
	go func() {
		log.Printf("HTTP server started on http://localhost:%d\n", cfg.HTTP.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v\n", err)
		}
	}()

	// 9. Graceful shutdown - ждем сигнал завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Recived signal: %v. Shutting down...\n", sig)

	// 8. Останавливаем все
	kafkaCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, cfg.HTTP.WriteTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v\n", err)
	}

	log.Println("Service stopped graceffuly!")
}
