package queue

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

func createProcessor[T any](index int) Processor[T] {
	return Processor[T]{
		handlerChan: make(chan func(T) error),
		index:       index,
	}
}

type Processor[T any] struct {
	index       int
	handlers    []func(T) error
	handlerChan chan func(T) error
}

func (processor Processor[T]) AddHandler(handler func(T) error) {
	processor.handlerChan <- handler
}

func (processor Processor[T]) Start(process chan RawJob) {
	for {
		select {
		case h := <-processor.handlerChan:
			processor.handlers = append(processor.handlers, h)
			break
		case raw := <-process:
			job, err := processor.rawToJob(raw)
			if err != nil {
				continue
			}
			processor.doProcessJob(job, processor.handlers)
			break
		}
	}
}

func (processor Processor[T]) rawToJob(raw RawJob) (Job[T], error) {
	job := Job[T]{
		Id:        raw.Id,
		Name:      raw.Name,
		CreatedAt: raw.CreatedAt,
		Tries:     raw.Tries,
	}
	value := new(T)
	err := json.Unmarshal(raw.Payload, &value)
	if err != nil {
		return Job[T]{}, err
	}
	job.Payload = *value
	return job, nil
}

func (processor Processor[T]) doProcessJob(job Job[T], handlers []func(payload T) error) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	go func(cancel context.Context) {
		// Ping the job every 30s, so we can ensure it is still being processed
		tick := time.NewTicker(time.Second * 30)
		for {
			select {
			case _ = <-tick.C:
				job.Ping()
				tick.Reset(time.Second * 30)
			case <-cancel.Done():
				return
			}
		}
	}(ctx)

	// TODO should each handler be its own job ?
	for _, f := range handlers {
		wg.Add(1)
		f := f
		go func() {
			defer wg.Done()
			err := f(job.Payload)
			if err != nil {
				job.Fail(err)
			}
		}()
	}

	job.Ping()
	wg.Wait()
	cancel()
	job.Complete()
}
