// Package writer - пакетная запись собранных записей в JSON-файл.
//
// Запись по одной записи означает одну операцию записи на запись.
// Здесь записи накапливаются и пишутся пачками: либо набралось указанное
// число записей, либо прошёл интервал ожидания. Цель - снизить число
// операций записи.
//
// Между накопителем и файлом нет промежуточного буфера намеренно: такой
// буфер сам объединял бы записи в блоки, и разница между режимами стала бы
// незаметной. Одна операция записи здесь - это одна операция записи в
// файл.
package writer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/frfn0/LAB14DataPipeline/internal/owm"
)

// Options - параметры пакетной записи.
type Options struct {
	// BatchSize - сколько записей накапливать перед записью. Единица
	// означает режим без пакетной записи: полезно для сравнения.
	BatchSize int
	// FlushInterval - как часто дописывать накопленное, даже если пачка
	// ещё не набралась. Ноль отключает запись по времени.
	FlushInterval time.Duration
}

// Stats - счётчики записи для оценки выигрыша от буферизации.
type Stats struct {
	// Records - сколько записей принято.
	Records int64
	// WriteOps - сколько операций записи выполнено. Это и есть то, что
	// сокращает пакетная запись.
	WriteOps int64
	// Bytes - сколько байт записано.
	Bytes int64
	// BySize - сколько пачек записано по достижении размера.
	BySize int64
	// ByTime - сколько пачек записано по таймеру.
	ByTime int64
	// ByClose - сколько пачек записано при завершении.
	ByClose int64
	// MaxBatch - наибольший размер записанной пачки.
	MaxBatch int
}

// AvgBatchSize - средний размер пачки.
func (s Stats) AvgBatchSize() float64 {
	if s.WriteOps == 0 {
		return 0
	}

	return float64(s.Records) / float64(s.WriteOps)
}

// Validate проверяет параметры записи.
func (o Options) Validate() error {
	if o.BatchSize < 1 {
		return fmt.Errorf("размер пачки должен быть не меньше 1, получено %d", o.BatchSize)
	}

	if o.FlushInterval < 0 {
		return fmt.Errorf("интервал сброса не может быть отрицательным: %v", o.FlushInterval)
	}

	return nil
}

// Collector читает записи из канала и пишет их пачками.
//
// Работает в одной горутине: и запись, и выбор между каналом и таймером
// происходят последовательно, поэтому блокировки не нужны.
type Collector struct {
	sink    io.Writer
	opts    Options
	stats   Stats
	batch   []owm.WeatherRecord
	scratch []byte
	err     error
}

// New создаёт сборщик записей поверх писателя.
//
// Args:
//
//	sink: куда писать; обычно файл.
//	opts: параметры пакетной записи.
//
// Returns:
//
//	Готовый сборщик или ошибка при неверных параметрах.
func New(sink io.Writer, opts Options) (*Collector, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &Collector{
		sink:  sink,
		opts:  opts,
		batch: make([]owm.WeatherRecord, 0, opts.BatchSize),
	}, nil
}

// Run читает записи из канала до его закрытия.
//
// Обычное завершение - закрытие records: сначала дописывается всё,
// что осталось. Канал force - аварийный выход, он закрывается, когда
// ждать больше нечего: накопленное дописывается и Run возвращается.
//
// Пачка пишется в трёх случаях: набрался размер, сработал таймер, канал
// закрыт. Таймер нужен для редкого потока: если новые записи приходят
// медленнее, чем размер пачки, накопленное не зависло бы до конца сбора.
//
// Args:
//
//	records: канал с записями.
//	force: канал аварийной остановки.
//	    возвращается. Закрытие records тоже останавливает Run.
func (c *Collector) Run(records <-chan owm.WeatherRecord, force <-chan struct{}) {
	var ticker *time.Ticker
	var tick <-chan time.Time

	if c.opts.FlushInterval > 0 {
		ticker = time.NewTicker(c.opts.FlushInterval)
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		select {
		case record, ok := <-records:
			if !ok {
				c.flush("close")
				return
			}

			c.add(record)

		case <-tick:
			c.flush("time")

		case <-force:
			// Аварийная остановка: забираем всё, что уже положили в
			// канал, и дописываем. Обычное завершение идёт по закрытию
			// records и не теряет ничего.
			c.drain(records)
			c.flush("close")
			return
		}
	}
}

// drain забирает из канала всё, что в нём уже лежит, не ожидая новых.
//
// Задания ещё выполняются не будут: drain ничего не ждёт, а забирает
// только готовое. Благодаря add внутри действует и запись по размеру,
// поэтому канал с большим остатком записей не превращается в одну
// огромную пачку.
func (c *Collector) drain(records <-chan owm.WeatherRecord) {
	for {
		select {
		case record, ok := <-records:
			if !ok {
				return
			}
			c.add(record)
		default:
			return
		}
	}
}

// add добавляет запись в накопитель и при необходимости пишет пачку.
func (c *Collector) add(record owm.WeatherRecord) {
	c.batch = append(c.batch, record)
	c.stats.Records++

	if len(c.batch) >= c.opts.BatchSize {
		c.flush("size")
	}
}

// flush пишет накопленную пачку одной операцией.
//
// Причина попадает только в счётчики: она показывает, чем вызвана запись,
// и позволяет понять, какой из двух механизмов сработал.
func (c *Collector) flush(reason string) {
	if len(c.batch) == 0 {
		return
	}

	c.scratch = c.scratch[:0]

	for _, record := range c.batch {
		payload, err := json.Marshal(record)
		if err != nil {
			// Одна непереносимая запись не должна терять всю пачку.
			continue
		}

		c.scratch = append(c.scratch, payload...)
		c.scratch = append(c.scratch, '\n')
	}

	if len(c.scratch) > 0 {
		written, err := c.sink.Write(c.scratch)
		if err != nil {
			// Ошибка запоминается и возвращается через Err, но сбор не
			// останавливается: оставшиеся записи всё равно попробовать
			// записать лучше, чем бросить всё.
			c.remember(err)
		} else {
			c.stats.WriteOps++
			c.stats.Bytes += int64(written)
			if len(c.batch) > c.stats.MaxBatch {
				c.stats.MaxBatch = len(c.batch)
			}
		}
	}

	switch reason {
	case "size":
		c.stats.BySize++
	case "time":
		c.stats.ByTime++
	case "close":
		c.stats.ByClose++
	}

	c.batch = c.batch[:0]
}

// remember сохраняет первую ошибку записи.
func (c *Collector) remember(err error) {
	if c.err == nil {
		c.err = err
	}
}

// Err возвращает первую ошибку записи, если она была.
func (c *Collector) Err() error {
	return c.err
}

// Stats - накопленные счётчики записи.
func (c *Collector) Stats() Stats {
	return c.stats
}

// ErrWrite - ошибка записи с указанием пачки.
//
// Оборачивается в collector.Err, чтобы в логе было видно, в какой момент
// запись сорвалась.
var ErrWrite = errors.New("запись не удалась")
