package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wb-order-service/internal/cache"
	"wb-order-service/internal/handler"
	kafkaconsumer "wb-order-service/internal/kafka"
	"wb-order-service/internal/repository"
)

func main() {
	log.Println("Starting Order Service...")

	//1. Подключение к БД Postgres
	ctx := context.Background()
	connString := "postgres://wb_user:wb_password@localhost:5432/wb_orders"

	repo, err := repository.NewPostgresRepository(ctx, connString)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v\n", err)
	}
	defer repo.Close()
	log.Println("Connected to PostgreSQL")

	// 2.Создаем кэш
	orderCache := cache.NewOrderCache()

	// 3. Восстанавливаем кэш из БД
	orders, err := repo.GetAllOrders(ctx)
	if err != nil {
		log.Printf("Warning: could not restore cache from DB: %v\n", err)
	} else {
		for _, order := range orders {
			orderCache.Set(order)
		}
		log.Printf("Cache restored: %d orders loaded from DB\n", orderCache.Size())
	}

	//4. Запускаем Kafka Consumer в отдельной горутине
	kafkaCtx, kafkaCancel := context.WithCancel(ctx)
	defer kafkaCancel()

	consumer := kafkaconsumer.NewConsumer(
		[]string{"localhost:9092"}, // Броке Kafka
		"orders",
		repo,
		orderCache,
	)
	defer consumer.Close()

	go consumer.Run(kafkaCtx)
	log.Println("Kafka consumer started")

	//5. Настраиваем HTTP-сервер
	h := handler.NewHandler(repo, orderCache)
	router := h.NewRouter()

	server := &http.Server{
		Addr:         ":8081",
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 6. Запускаем HTTP-сервер в отдельной горутине
	go func() {
		log.Println("HTTP server started on http://localhost:8081")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v\n", err)
		}
	}()

	// 7. Graceful shutdown - ждем сигнал завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Recived signal: %v. Shutting down...\n", sig)

	// 8. Останавливаем все
	kafkaCancel() // останавливаем Kafka consumer

	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v\n", err)
	}

	log.Println("Service stopped graceffuly!")
}
