package models

import "errors"

// Sentinel-ошибки (заранее объявленные значения ошибок) доменного слоя.
// Репозиторий переводит в них ошибки драйвера БД, а сервис и консьюмер
// проверяют их через errors.Is — и ничего не знают о pgx и кодах PostgreSQL.

// ErrOrderExists — заказ с таким order_uid уже сохранён.
// Для консьюмера это не ошибка: повторное сообщение просто пропускаем.
var ErrOrderExists = errors.New("order already exists")

// ErrOrderNotFound — заказа с таким order_uid нет в БД.
var ErrOrderNotFound = errors.New("order not found")

// ErrInvalidOrder — БД отвергла сами данные заказа (слишком длинная строка,
// нарушение ограничения и т.п.). Повтор ничего не изменит — такое сообщение уходит в DLQ.
// Отличается от временных ошибок (БД недоступна, таймаут), которые нужно повторять.
var ErrInvalidOrder = errors.New("order rejected by database")
