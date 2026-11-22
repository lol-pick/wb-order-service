package models

import "errors"

// ErrOrderExists — заказ с таким order_uid уже сохранён -> пропускаем.
var ErrOrderExists = errors.New("order already exists")

// ErrOrderNotFound — заказа с таким order_uid нет в БД.
var ErrOrderNotFound = errors.New("order not found")

// ErrInvalidOrder — БД отвергла сами данные заказа — такое сообщение уходит в DLQ.
var ErrInvalidOrder = errors.New("order rejected by database")
