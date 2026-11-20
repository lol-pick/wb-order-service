package repository

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"wb-order-service/internal/models"
)

// Юнит-тест без БД: классификация ошибок записи по коду PostgreSQL
func TestWriteErr(t *testing.T) {
	tests := []struct {
		code        string
		wantInvalid bool
	}{
		{"22001", true},  // string_data_right_truncation: строка длиннее varchar(N)
		{"23502", true},  // not_null_violation
		{"08006", false}, // connection_failure — временная
		{"57P01", false}, // admin_shutdown — БД останавливают, временная
	}
	for _, tt := range tests {
		pgErr := &pgconn.PgError{Code: tt.code}
		err := writeErr("insert", pgErr)

		if got := errors.Is(err, models.ErrInvalidOrder); got != tt.wantInvalid {
			t.Errorf("code %s: errors.Is(ErrInvalidOrder) = %v, want %v", tt.code, got, tt.wantInvalid)
		}
		// Исходная ошибка pgx тоже должна находиться по цепочке
		var target *pgconn.PgError
		if !errors.As(err, &target) {
			t.Errorf("code %s: original *pgconn.PgError lost", tt.code)
		}
	}

	// Ошибка не от PostgreSQL (например, обрыв сети) — временная
	if errors.Is(writeErr("insert", errors.New("i/o timeout")), models.ErrInvalidOrder) {
		t.Error("non-postgres error must not be ErrInvalidOrder")
	}
}
