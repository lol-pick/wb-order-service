package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Тестовые данные для генерации случайных заказов
var (
	names    = []string{"Ivan Petro", "Varia Sidorova", "Alex Ivanov", "Elena Novikova", "Dmitry Volkov"}
	cities   = []string{"Moscow", "Saint Petersburg", "Aktobe", "Kazan", "Sochi"}
	brands   = []string{"Nike", "Adidas", "Reebook", "Puma", "New Balance"}
	products = []string{"Sneakers", "T-Shirt", "Jacket", "Backpack", "Cap"}
)

func main() {
	// Подключаемся к Kafka
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP("localhost:9092"),
		Topic:        "orders",
		Balancer:     &kafkago.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	log.Println("Producer started. Sending orders to Kafka...")

	// Отправляем 5 тестовых заказов
	for i := range 5 {
		order := generateOrder(i)

		// Превращаем структуру в JSON формат
		data, err := json.Marshal(order)
		if err != nil {
			log.Printf("Error marshaling order: %v\n", err)
			continue
		}

		// Отправляем в Kafka
		err = writer.WriteMessages(context.Background(),
			kafkago.Message{
				Key:   []byte(order["order_uid"].(string)),
				Value: data,
			},
		)
		if err != nil {
			log.Printf("Error sending message: %v\n", err)
			continue
		}

		log.Printf("Sent order: %s\n", order["order_uid"])
		time.Sleep(1 * time.Second) // пауза между отправками
	}

	log.Println("All orders sent!")
}

// generateOrder создаем случайный заказ
func generateOrder(index int) map[string]any {
	uid := fmt.Sprintf("order-%d-%d", index, time.Now().UnixNano())
	return map[string]any{
		"order_uid":          uid,
		"track_number":       fmt.Sprintf("TRACK%d", rand.Intn(9999)),
		"entry":              "WBIL",
		"locale":             "en",
		"internal_signature": "",
		"customer_id":        fmt.Sprintf("customer-%d", rand.Intn(1000)),
		"delivery_service":   "meest",
		"shardkey":           "9",
		"sm_id":              rand.Intn(100),
		"date_created":       time.Now().Format(time.RFC3339),
		"oof_shard":          "1",
		"delivery": map[string]any{
			"name":    names[rand.Intn(len(names))],
			"phone":   fmt.Sprintf("+7%010d", rand.Intn(9999999999)),
			"zip":     fmt.Sprintf("%06d", rand.Intn(999999)),
			"city":    cities[rand.Intn(len(cities))],
			"address": fmt.Sprintf("Street %d, apt %d", rand.Intn(100), rand.Intn(200)),
			"region":  "Central",
			"email":   fmt.Sprintf("user%d@test.com", rand.Intn(1000)),
		},
		"payment": map[string]any{
			"transaction":   uid,
			"request_id":    "",
			"currency":      "RUB",
			"provider":      "wbpay",
			"amount":        rand.Intn(10000) + 100,
			"payment_dt":    time.Now().Unix(),
			"bank":          "alpha",
			"delivery_cost": rand.Intn(2000),
			"goods_total":   rand.Intn(5000) + 100,
			"custom_fee":    0,
		},
		"items": []map[string]any{
			{
				"chrt_id":      rand.Intn(9999999),
				"track_number": fmt.Sprintf("TRACK%d", rand.Intn(99999)),
				"price":        rand.Intn(5000) + 100,
				"rid":          fmt.Sprintf("rid-%d", rand.Intn(99999)),
				"name":         products[rand.Intn(len(products))],
				"sale":         rand.Intn(50),
				"size":         fmt.Sprintf("%d", rand.Intn(50)+30),
				"total_price":  rand.Intn(5000) + 100,
				"nm_id":        rand.Intn(9999999),
				"brand":        brands[rand.Intn(len(brands))],
				"status":       202,
			},
		},
	}
}
