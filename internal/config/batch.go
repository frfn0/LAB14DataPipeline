package config

import (
	"errors"
	"time"
)

// Параметры пакетной записи по умолчанию.
//
// Значения подобраны под сбор десяти городов: 41 запись на город приходит
// меньше чем за две секунды, поэтому пачка в 50 записей набирается за
// один-два города, а интервал в 250 мс не даёт ждать хвост пачки слишком
// долго.
const (
	DefaultBatchSize     = 50
	DefaultFlushInterval = 250 * time.Millisecond
	DefaultChannelBuffer = 256
)

// Batch - параметры буферизации и пакетной записи.
type Batch struct {
	// BatchSize - сколько записей накапливать перед записью.
	BatchSize int
	// FlushInterval - запись по таймеру, даже если пачка не набралась.
	FlushInterval time.Duration
	// ChannelBuffer - ёмкость канала между сбором и записью.
	ChannelBuffer int
}

// Validate проверяет параметры записи.
func (b Batch) Validate() error {
	if b.BatchSize < 1 {
		return errors.New("размер пачки должен быть не меньше 1")
	}

	if b.FlushInterval < 0 {
		return errors.New("интервал сброса не может быть отрицательным")
	}

	if b.ChannelBuffer < 1 {
		return errors.New("буфер канала должен быть не меньше 1")
	}

	return nil
}

// DefaultBatch - параметры записи по умолчанию.
func DefaultBatch() Batch {
	return Batch{
		BatchSize:     DefaultBatchSize,
		FlushInterval: DefaultFlushInterval,
		ChannelBuffer: DefaultChannelBuffer,
	}
}
