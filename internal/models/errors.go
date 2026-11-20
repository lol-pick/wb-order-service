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
