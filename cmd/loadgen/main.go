// loadgen — инструменты для замеров сервиса: нагрузка на Kafka, наполнение БД,
// время старта и выгрузка order_uid для k6.
//
// Не трогает код сервиса, поэтому одинаково работает на main («до») и на ветке с изменениями («после»).
// Настройки Kafka и PostgreSQL берёт из тех же переменных окружения (.env), что и сервис.
//
//	go run ./cmd/loadgen produce -n 10000      // пропускная способность консьюмера
//	go run ./cmd/loadgen seed -n 100000        // наполнить БД для замера старта
//	go run ./cmd/loadgen startup -bin ./bin/service
//	go run ./cmd/loadgen ids -n 1000           // order_uid для k6
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	kafkago "github.com/segmentio/kafka-go"

	"wb-order-service/internal/config"
)

func main() {
	_ = godotenv.Load()
	log.SetFlags(log.Ltime)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "produce":
		err = runProduce(cfg, args)
	case "seed":
		err = runSeed(cfg, args)
	case "startup":
		err = runStartup(args)
	case "ids":
		err = runIDs(cfg, args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatalf("%s: %v", cmd, err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: loadgen <command> [flags]

commands:
  produce  отправить N заказов в Kafka и замерить, за сколько консьюмер сохранит их в БД
  seed     вставить N заказов прямо в PostgreSQL (для замера времени старта)
  startup  запустить бинарник сервиса и замерить время до готовности /health
  ids      выгрузить последние N order_uid в файл для k6

"loadgen <command> -h" — флаги команды`)
}

// ---------- produce ----------

// runProduce шлёт N заказов в топик и ждёт, пока все появятся в БД.
// Пропускная способность = N / (время от начала отправки до последнего заказа в БД).
// Отправка батчами занимает доли секунды, консьюмер гораздо медленнее,
// поэтому результат почти целиком — скорость консьюмера.
func runProduce(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	n := fs.Int("n", 10000, "сколько заказов отправить")
	items := fs.Int("items", 3, "товаров в каждом заказе")
	timeout := fs.Duration("timeout", 10*time.Minute, "сколько ждать сохранения всех заказов")
	_ = fs.Parse(args)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.Postgres.ConnString())
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	// Уникальный префикс прогона: считаем в БД только свои заказы
	prefix := fmt.Sprintf("lg-%d-", time.Now().Unix())

	dlqBefore, dlqErr := topicSize(ctx, cfg.Kafka.Brokers[0], cfg.Kafka.DLQTopic)

	// Готовим все сообщения заранее — генерация JSON не попадает в замер
	msgs := make([]kafkago.Message, *n)
	for i := range msgs {
		uid := fmt.Sprintf("%s%d", prefix, i)
		data, err := json.Marshal(generateOrder(uid, *items))
		if err != nil {
			return fmt.Errorf("marshal order: %w", err)
		}
		msgs[i] = kafkago.Message{Key: []byte(uid), Value: data}
	}

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.Topic,
		Balancer:     &kafkago.LeastBytes{},
		BatchSize:    1000,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
	}
	defer func() { _ = writer.Close() }()

	log.Printf("sending %d orders (%d items each) to topic %q...", *n, *items, cfg.Kafka.Topic)
	start := time.Now()
	const chunk = 1000
	for i := 0; i < len(msgs); i += chunk {
		end := min(i+chunk, len(msgs))
		if err := writer.WriteMessages(ctx, msgs[i:end]...); err != nil {
			return fmt.Errorf("write messages: %w", err)
		}
	}
	log.Printf("sent in %v, waiting for the consumer...", time.Since(start).Round(time.Millisecond))

	// Опрашиваем БД, пока не появятся все N заказов.
	// Ошибку запроса не считаем фатальной: в тесте надёжности БД специально останавливают,
	// и loadgen должен дождаться её возвращения, а не упасть. pgxpool сам переподключится.
	deadline := time.Now().Add(*timeout)
	var stored int
	for {
		var cnt int
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM orders WHERE order_uid LIKE $1`, prefix+"%").Scan(&cnt)
		if err != nil {
			log.Printf("  count failed (DB unavailable?): %v", err)
		} else {
			stored = cnt
			if stored >= *n {
				break
			}
			log.Printf("  stored %d / %d", stored, *n)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	elapsed := time.Since(start)

	// Товаров должно быть ровно N*items: меньше — потеря, больше — дубли
	var storedItems int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM items WHERE order_uid LIKE $1`, prefix+"%").Scan(&storedItems)
	if err != nil {
		return fmt.Errorf("count items: %w", err)
	}

	fmt.Println()
	fmt.Printf("orders sent:      %d\n", *n)
	fmt.Printf("orders in DB:     %d\n", stored)
	fmt.Printf("items in DB:      %d (expected %d)\n", storedItems, *n**items)
	if dlqErr == nil {
		if dlqAfter, err := topicSize(ctx, cfg.Kafka.Brokers[0], cfg.Kafka.DLQTopic); err == nil {
			fmt.Printf("sent to DLQ:      %d\n", dlqAfter-dlqBefore)
		}
	}
	fmt.Printf("elapsed:          %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:       %.0f orders/s\n", float64(stored)/elapsed.Seconds())

	if stored < *n {
		return fmt.Errorf("timeout: only %d of %d orders stored", stored, *n)
	}
	return nil
}

// topicSize — сколько сообщений лежит в партиции 0 топика (у нас одна партиция)
func topicSize(ctx context.Context, broker, topic string) (int64, error) {
	conn, err := kafkago.DialLeader(ctx, "tcp", broker, topic, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	first, last, err := conn.ReadOffsets()
	if err != nil {
		return 0, err
	}
	return last - first, nil
}

var (
	names    = []string{"Ivan Petrov", "Varia Sidorova", "Alex Ivanov", "Elena Novikova", "Dmitry Volkov"}
	cities   = []string{"Moscow", "Saint Petersburg", "Aktobe", "Kazan", "Sochi"}
	brands   = []string{"Nike", "Adidas", "Reebok", "Puma", "New Balance"}
	products = []string{"Sneakers", "T-Shirt", "Jacket", "Backpack", "Cap"}
)

// generateOrder — случайный заказ в формате топика orders (как в cmd/producer)
func generateOrder(uid string, itemsCount int) map[string]any {
	items := make([]map[string]any, itemsCount)
	for i := range items {
		items[i] = map[string]any{
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
		}
	}
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
			"phone":   fmt.Sprintf("+7%010d", rand.Intn(999999999)),
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
		"items": items,
	}
}

// ---------- seed ----------

// runSeed вставляет N заказов прямо в БД одним INSERT ... SELECT FROM generate_series на таблицу.
// Через Kafka 100 000 заказов шли бы долго, а для замера старта важны только данные в БД.
func runSeed(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	n := fs.Int("n", 100000, "сколько заказов вставить")
	items := fs.Int("items", 3, "товаров в каждом заказе")
	_ = fs.Parse(args)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.Postgres.ConnString())
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	prefix := fmt.Sprintf("seed-%d-", time.Now().Unix())
	start := time.Now()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // после Commit — no-op

	// date_created уходит в прошлое на g секунд: у заказов разные даты, свежие — с малым g
	queries := []string{
		`INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature,
			customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
		SELECT $1::text || g, 'TRACK' || g, 'WBIL', 'en', '', 'customer-' || (g % 1000),
			'meest', '9', g % 100, now() - make_interval(secs => g), '1'
		FROM generate_series(1, $2::int) g`,
		`INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
		SELECT $1::text || g, 'Ivan Petrov', '+79990000000', '123456', 'Moscow',
			'Street ' || g, 'Central', 'user' || g || '@test.com'
		FROM generate_series(1, $2::int) g`,
		`INSERT INTO payments (order_uid, transaction, request_id, currency, provider, amount,
			payment_dt, bank, delivery_cost, goods_total, custom_fee)
		SELECT $1::text || g, $1::text || g, '', 'RUB', 'wbpay', 1000 + g % 9000,
			extract(epoch FROM now())::bigint, 'alpha', 500, 1500, 0
		FROM generate_series(1, $2::int) g`,
		`INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale,
			size, total_price, nm_id, brand, status)
		SELECT $1::text || g, g * 10 + i, 'TRACK' || g, 1000, 'rid-' || g || '-' || i, 'Sneakers',
			10, '42', 900, g * 10 + i, 'Nike', 202
		FROM generate_series(1, $2::int) g, generate_series(1, $3::int) i`,
	}
	for i, q := range queries {
		qargs := []any{prefix, *n}
		if i == len(queries)-1 {
			qargs = append(qargs, *items)
		}
		if _, err := tx.Exec(ctx, q, qargs...); err != nil {
			return fmt.Errorf("seed query %d: %w", i+1, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// ANALYZE обновляет статистику — планировщик сразу видит реальный размер таблиц
	if _, err := pool.Exec(ctx, `ANALYZE orders, deliveries, payments, items`); err != nil {
		return fmt.Errorf("analyze: %w", err)
	}

	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&total); err != nil {
		return fmt.Errorf("count: %w", err)
	}
	log.Printf("inserted %d orders (%d items each) in %v; orders in DB now: %d",
		*n, *items, time.Since(start).Round(time.Millisecond), total)
	return nil
}

// ---------- startup ----------

// runStartup запускает бинарник сервиса и меряет время до первого успешного /health.
// HTTP-сервер стартует после прогрева кэша, поэтому это время = подготовка + прогрев.
func runStartup(args []string) error {
	fs := flag.NewFlagSet("startup", flag.ExitOnError)
	bin := fs.String("bin", "./bin/service", "путь к собранному бинарнику сервиса")
	health := fs.String("health", "http://localhost:8081/health", "URL health check")
	timeout := fs.Duration("timeout", 10*time.Minute, "сколько ждать готовности")
	_ = fs.Parse(args)

	// Сервис наследует окружение loadgen (в том числе переменные из .env)
	cmd := exec.Command(*bin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	start := time.Now()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", *bin, err)
	}
	defer stopProcess(cmd)

	client := &http.Client{Timeout: time.Second}
	deadline := start.Add(*timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(*health)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Printf("\nready in %v\n", time.Since(start).Round(time.Millisecond))
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("service did not become ready before timeout")
}

// stopProcess останавливает сервис: на Linux/macOS — SIGINT (graceful shutdown),
// на Windows сигналы процессу не отправить, поэтому Kill.
func stopProcess(cmd *exec.Cmd) {
	if runtime.GOOS == "windows" {
		_ = cmd.Process.Kill()
	} else {
		_ = cmd.Process.Signal(os.Interrupt)
	}
	_ = cmd.Wait()
}

// ---------- ids ----------

// runIDs выгружает последние N order_uid — k6 запрашивает их случайным образом
func runIDs(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("ids", flag.ExitOnError)
	n := fs.Int("n", 1000, "сколько order_uid выгрузить")
	out := fs.String("out", "loadtest/k6/ids.txt", "файл для записи")
	_ = fs.Parse(args)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.Postgres.ConnString())
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx,
		`SELECT order_uid FROM orders ORDER BY date_created DESC LIMIT $1`, *n)
	if err != nil {
		return fmt.Errorf("select ids: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return fmt.Errorf("scan id: %w", err)
		}
		ids = append(ids, uid)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate ids: %w", err)
	}
	if len(ids) == 0 {
		return errors.New("no orders in DB: run produce or seed first")
	}

	if err := os.WriteFile(*out, []byte(strings.Join(ids, "\n")+"\n"), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", *out, err)
	}
	log.Printf("wrote %d ids to %s", len(ids), *out)
	return nil
}
