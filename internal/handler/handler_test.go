package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"wb-order-service/internal/models"
)

// stubService — сервис с заранее заданным ответом GetOrder
type stubService struct {
	order models.Order
	found bool
	err   error
}

func (s *stubService) SaveOrder(_ context.Context, _ models.Order) error { return nil }
func (s *stubService) GetOrder(_ context.Context, _ string) (models.Order, bool, error) {
	return s.order, s.found, s.err
}
func (s *stubService) RestoreCache(_ context.Context, _ int) error { return nil }

// do прогоняет запрос через настоящий роутер (с middleware), без запуска сервера.
// httptest.NewRecorder записывает ответ в память.
func do(t *testing.T, svc *stubService, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	router := NewHandler(svc).NewRouter()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestGetOrder_Found(t *testing.T) {
	svc := &stubService{order: models.Order{OrderUID: "abc", TrackNumber: "T1"}, found: true}

	rec := do(t, svc, http.MethodGet, "/order/abc")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %q", ct)
	}
	var got models.Order
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.OrderUID != "abc" || got.TrackNumber != "T1" {
		t.Errorf("unexpected order: %+v", got)
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	rec := do(t, &stubService{found: false}, http.MethodGet, "/order/missing")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	assertErrorBody(t, rec, "order not found")
}

// Ошибка БД → 500, и текст ошибки БД не утекает клиенту
func TestGetOrder_InternalError(t *testing.T) {
	rec := do(t, &stubService{err: errors.New("connection refused to 10.0.0.5:5432")}, http.MethodGet, "/order/abc")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Error("internal error details leaked to the client")
	}
	assertErrorBody(t, rec, "internal server error")
}

func TestHealth(t *testing.T) {
	rec := do(t, &stubService{}, http.MethodGet, "/health")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil || body["status"] != "ok" {
		t.Fatalf(`expected {"status":"ok"}, got %q (err %v)`, rec.Body.String(), err)
	}
}

// Метка path в HTTP-метриках — шаблон маршрута, а не реальный путь.
// Иначе каждый order_uid создавал бы новый временной ряд (взрыв кардинальности).
func TestMetrics_PathLabelIsRoutePattern(t *testing.T) {
	svc := &stubService{order: models.Order{OrderUID: "uid-for-metrics-test"}, found: true}
	do(t, svc, http.MethodGet, "/order/uid-for-metrics-test")
	do(t, svc, http.MethodGet, "/no-such-path-metrics-test")

	rec := do(t, svc, http.MethodGet, "/metrics")
	body, _ := io.ReadAll(rec.Body)
	metrics := string(body)

	if !strings.Contains(metrics, `path="/order/{order_uid}"`) {
		t.Error(`expected path="/order/{order_uid}" in metrics`)
	}
	if strings.Contains(metrics, "uid-for-metrics-test") {
		t.Error("raw order_uid must not appear in metric labels")
	}
	if strings.Contains(metrics, "no-such-path-metrics-test") {
		t.Error("unknown paths must be grouped, not stored as labels")
	}
	if !strings.Contains(metrics, `path="unmatched"`) {
		t.Error(`expected path="unmatched" for unknown routes`)
	}
}

func assertErrorBody(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("invalid JSON error body: %v", err)
	}
	if body["error"] != want {
		t.Errorf("expected error %q, got %q", want, body["error"])
	}
}
