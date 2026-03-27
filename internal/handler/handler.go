package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"

	"wb-order-service/internal/service"
)

var tracer = otel.Tracer("handeler")

// Handler - обработчик HTTP-запросов
type Handler struct {
	service service.OrderService
}

// NewHandler - создает новый обработчки
func NewHandler(svc service.OrderService) *Handler {
	return &Handler{
		service: svc,
	}
}

// NewRouter создает и настраивает новый роуте
func (h *Handler) NewRouter() *chi.Mux {
	r := chi.NewRouter()

	// Middleware
	r.Use(skipPathLogger("/metrics"))
	r.Use(middleware.Recoverer) // Ловит паники, не дает серверу упасть
	r.Use(metricsMiddleware)

	// Маршруты
	r.Get("/order/{order_uid}", h.GetOrder) // GET /order/uid
	r.Handle("/metrics", PrometheusHandler())
	r.Get("/health", h.Health)
	r.Get("/", h.IndexPage) // GET / - главная страница

	// Отдаем статические файлы (HTML, CSS, JS)
	fileServer := http.FileServer(http.Dir("web"))
	r.Handle("/static/*", http.StripPrefix("/static", fileServer))

	// Создание иконки
	r.Get("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/favicon.ico")
	})

	return r
}

// skipPathLogger логирует все запросы КРОМЕ указанного пути
func skipPathLogger(skipPath string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == skipPath {
				next.ServeHTTP(w, r)
				return
			}
			middleware.Logger(next).ServeHTTP(w, r)
		})
	}
}

// GetOrder - обработчик GET запроса /order/{order_uid}
func (h *Handler) GetOrder(w http.ResponseWriter, r *http.Request) {
	// Создаем span для трейсинга
	ctx, span := tracer.Start(r.Context(), "handler.GetOrder")
	defer span.End()

	// 1. Извлекаем order_uid из URL
	orderUID := chi.URLParam(r, "order_uid")
	if orderUID == "" {
		respondError(w, http.StatusBadRequest, "order_uid is required")
		return
	}

	// 2. Ищем в кеше
	order, found, err := h.service.GetOrder(ctx, orderUID)
	if err != nil {
		log.Printf("Error getting order: %v\n", err)
		respondError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	if !found {
		RecordCacheMiss()
		respondError(w, http.StatusNotFound, "order not found")
		return
	}

	// 3. Возвращаем JSON
	RecordCacheHit()
	respondJSON(w, http.StatusOK, order)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// IndexPage - отдает главную HTML-страницу
func (h *Handler) IndexPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/index.html")
}

// responJSON - вспомогательная функция для отправки JSON-ответов
func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		log.Printf("Error encoding JSON response: %v\n", err)
	}
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
