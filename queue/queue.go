package queue

import (
	"context"
	"errors"
	"fmt"
)

type Queue[T any] struct {
	listenerChan chan RawJob
	workers      int
	processors   []Processor[T]
	stop         chan bool
	context      context.Context
	cancel       context.CancelFunc
}

type CreateOptions struct {
	Workers int
}

var poller = createPoller()
var notify = createNotify(poller)

func CreateWithOptions[T any](options CreateOptions) Queue[T] {
	if options.Workers == 0 {
		options.Workers = 25
	}

	ctx, cancel := context.WithCancel(context.Background())

	processors := make([]Processor[T], options.Workers)

	for i := range processors {
		processors[i] = createProcessor[T](i)
	}

	queue := Queue[T]{
		listenerChan: make(chan RawJob, options.Workers),
		workers:      options.Workers,
		processors:   processors,
		context:      ctx,
		cancel:       cancel,
	}

	for _, processor := range processors {
		go processor.Start(queue.listenerChan)
	}

	queue.listen()

	return queue
}

// Create a new queue for a single event without a default handler
// the default handler will need to be set by calling the AddHandler method.
// If you do not specify a default handler, a warning will be logged to the console.
func Create[T any]() Queue[T] {
	return CreateWithOptions[T](CreateOptions{})
}

// CreateWithHandler Creates a new queue for a single event with a handler
func CreateWithHandler[T any](handler func(T) error) Queue[T] {
	queue := CreateWithOptions[T](CreateOptions{})
	queue.AddHandler(handler)
	return queue
}

func (queue Queue[T]) AddHandler(handler func(T) error) {
	for _, processor := range queue.processors {
		processor.AddHandler(handler)
	}
}

// listen start listening for pg changes for the exact event and pgNotify our Queue that the table has changed.
// The Queue will pgNotify the workers and a random worker will then know to pick up a new job
func (queue Queue[T]) listen() {
	event := fmt.Sprintf("%T", *new(T))
	poller.addHandler(PollerHandler{
		event:   event,
		handler: queue.listenerChan,
	})
}

func (queue Queue[T]) Enqueue(payload T) {
	event := fmt.Sprintf("%T", payload)
	processes <- EnqueueJob{
		Name:    event,
		Payload: payload,
	}
}

func Initialize() (Manager, error) {
	if manager.started {
		return manager, errors.New("initialize should only be called once")
	}
	startEnqueueListener()
	startFailureDetector()
	poller.start()
	manager.Start()
	return manager, notify.Start()
}
