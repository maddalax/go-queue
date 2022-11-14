package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"
	"time"
)

type Queue[T any] struct {
	Id           string
	listenerChan chan RawJob
	workers      int
	processors   []Processor[T]
	stop         chan bool
	context      context.Context
	cancel       context.CancelFunc
}

type RegisterQueue struct {
	bun.BaseModel `bun:"table:workers"`
	Id            string
	UpdatedAt     string `bun:"type:timestamp"`
	Name          string
}

type CreateOptions struct {
	Workers int
}

// var poller = createPoller()
var notify = createNotify()

func CreateWithOptions[T any](options CreateOptions) Queue[T] {
	if options.Workers == 0 {
		options.Workers = 50
	}

	id := uuid.NewString()

	ctx, cancel := context.WithCancel(context.Background())

	processors := make([]Processor[T], options.Workers)

	for i := range processors {
		processors[i] = createProcessor[T](i, id)
	}

	queue := Queue[T]{
		Id: id,
		// Allow a backlog of around 100 jobs per worker
		listenerChan: make(chan RawJob, options.Workers),
		workers:      options.Workers,
		processors:   processors,
		context:      ctx,
		cancel:       cancel,
	}

	queue.register()

	for _, processor := range processors {
		go processor.Start(queue.listenerChan)
	}

	queue.listen()

	manager.OnShutdown(func() {
		logger.Println("shutting down queue: " + queue.Id)
	})

	return queue
}

// Create a new queue for a single event without a default handler
// the default handler will need to be set by calling the addHandler method.
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

func (queue Queue[T]) register() {
	event := fmt.Sprintf("%T", *new(T))
	_, err := GetDatabase().NewInsert().Model(&RegisterQueue{
		Id:        queue.Id,
		UpdatedAt: time.Now().Format(time.RFC3339),
		Name:      event,
	}).Exec(context.Background())
	if err != nil {
		logger.Fatal(err)
	}
	manager.onQueueRegistered(queue.Id)
}

// listen start listening for pg changes for the exact event and pgNotify our Queue that the table has changed.
// The Queue will pgNotify the workers and a random worker will then know to pick up a new job
func (queue Queue[T]) listen() {
	event := fmt.Sprintf("%T", *new(T))
	notify.addHandler(NotifyHandler{
		id:    queue.Id,
		event: event,
		handler: func(job RawJob) {
			select {
			case queue.listenerChan <- job:
				println("enqueued")
				break
			default:
				println("unable to enqueue, workers full")

				//worker := RegisterQueue{}
				//err := GetDatabase().NewSelect().Model(&worker).Where("name = ? and id != ?", event, queue.Id).OrderExpr("random()").Limit(1).Scan(context.Background())
				//
				//if worker.Id == "" {
				//	logger.Println("did not find another worker to enqueue it to")
				//	return
				//}
				//
				//if err != nil {
				//	logger.Println(err.Error())
				//	return
				//}

				payload := EventPayload{
					Job:    job,
					Worker: queue.Id,
				}
				marshalled, _ := json.Marshal(payload)
				if err := pgdriver.Notify(context.Background(), GetDatabase(), "jobs:changed", string(marshalled)); err != nil {
					logger.Println(err.Error())
				}
			}
		},
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
	//poller.start()
	manager.Start()
	return manager, notify.Start()
}
