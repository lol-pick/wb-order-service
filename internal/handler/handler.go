package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"wb-order-service/internal/cache"
	"wb-order-service/internal/repository"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

// Handler - обработчик HTTP-запросов
type Handler struct {
	repo  *repository.PostgresRepository
	cache *cache.OrderCache
}

// NewHandler - создает новый обработчки
func NewHandler(repo *repository.PostgresRepository, cache *cache.OrderCache) *Handler {
	return &Handler{
		repo:  repo,
		cache: cache,
	}
}

// NewRouter создает и настраивает новый роуте
func (h *Handler) NewRouter() *chi.Mux {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Logger)    // Логирует каждый HTTP запрос
	r.Use(middleware.Recoverer) // Ловит паники, не дает серверу упасть

	// Маршруты
	r.Get("/order/{order_uid}", h.GetOrder) // GET /order/uid
	r.Get("/", h.IndexPage)                 // GET / - главная страница

	// Отдаем статические файлы (HTML, CSS, JS)
	fileServer := http.FileServer(http.Dir("web"))
	r.Handle("/static/*", http.StripPrefix("/static", fileServer))

	// Создание иконки
	r.Get("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/favicon.ico")
	})

	return r
}

// GetOrder - обработчик GET запроса /order/{order_uid}
func (h *Handler) GetOrder(w http.ResponseWriter, r *http.Request) {
	// 1. Извлекаем order_uid из URL
	orderUID := chi.URLParam(r, "order_uid")
	if orderUID == "" {
		http.Error(w, `{"error": "order_uid is required"}`, http.StatusBadRequest)
		return
	}

	// 2. Ищем в кеше
	order, found := h.cache.Get(orderUID)
	if found {
		log.Printf("Cache HIT for order %s\n", orderUID)
		responJSON(w, http.StatusOK, order)
		return
	}

	// 3. Не нашли в кэше - ищем в БД
	log.Printf("Cache MISS for order %s, querying DB\n", orderUID)
	order, err := h.repo.GetOrder(r.Context(), orderUID)
	if err != nil {
		// Проверяем: заказ не найден или ошибка в БД
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, `{"error": "order not found"}`, http.StatusNotFound)
			return
		}
		log.Printf("Error getting order from DB: %v\n", err)
		http.Error(w, `{"error": "internal server error"}`, http.StatusInternalServerError)
		return
	}

	// 4. Нашли в БД - кладем в кэш на будущее
	h.cache.Set(order)

	// 5. Возвращаем JSON
	responJSON(w, http.StatusOK, order)
}

// IndexPage - отдает главную HTML-страницу
func (h *Handler) IndexPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/index.html")
}

// responJSON - вспомогательная функция для отправки JSON-ответов
func responJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		log.Printf("Error encoding JSON response: %v\n", err)
	}
}
