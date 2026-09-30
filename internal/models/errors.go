package models

import "errors"

// ErrOrderExists — заказ с таким order_uid уже сохранён.
// Для консьюмера это не ошибка: повторное сообщение просто пропускаем.
var ErrOrderExists = errors.New("order already exists")
