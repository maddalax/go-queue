package queue

import (
	"context"
	"github.com/uptrace/bun/dialect/pgdialect"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Manager struct {
	started               bool
	onShutdown            chan bool
	onError               chan JobError
	onSuccess             chan JobSuccess
	onErrHandlerChan      chan func(error error, job JobForEvent)
	onSuccessHandlerChan  chan func(job JobForEvent)
	onShutdownHandlerChan chan func()
	onQueueRegisteredChan chan string
	queues                []string
	successHandlers       []func(job JobForEvent)
	errorHandlers         []func(error error, job JobForEvent)
	shutdownHandlers      []func()
}

type Count struct {
	Pending int
	Running int
	Failed  int
}

type Status struct {
	Counts map[string]Count
}

type statusQueryResult struct {
	Name   string
	Status string
	Count  int
}

func (manager Manager) Start() {
	if manager.started {
		return
	}
	manager.started = true

	ctx, _ := signal.NotifyContext(context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		<-ctx.Done()
		manager.shutdown()
		manager.onShutdown <- true
	}()

	go func() {
		ticker := time.NewTicker(time.Second * 30)

		for {
			select {
			case <-ticker.C:
				manager.ping()
				ticker.Reset(time.Second * 30)
			case handler := <-manager.onSuccessHandlerChan:
				manager.successHandlers = append(manager.successHandlers, handler)
				break
			case handler := <-manager.onShutdownHandlerChan:
				manager.shutdownHandlers = append(manager.shutdownHandlers, handler)
				break
			case handler := <-manager.onQueueRegisteredChan:
				manager.queues = append(manager.queues, handler)
				break
			case handler := <-manager.onErrHandlerChan:
				manager.errorHandlers = append(manager.errorHandlers, handler)
				break
			case _ = <-manager.onShutdown:
				logger.Println("shutting down")
				for _, handler := range manager.shutdownHandlers {
					handler()
				}
			case err := <-manager.onError:
				for _, handler := range manager.errorHandlers {
					handler(err.error, err.job)
				}
				break
			case success := <-manager.onSuccess:
				for _, handler := range manager.successHandlers {
					handler(success.job)
				}
				break
			}
		}
	}()
}

func (manager Manager) OnJobSuccess(callback func(job JobForEvent)) {
	manager.onSuccessHandlerChan <- callback
}

func (manager Manager) OnJobError(callback func(error error, job JobForEvent)) {
	manager.onErrHandlerChan <- callback
}

func (manager Manager) OnShutdown(callback func()) {
	manager.onShutdownHandlerChan <- callback
}

func (manager Manager) onQueueRegistered(id string) {
	manager.onQueueRegisteredChan <- id
}

func (manager Manager) Status() (Status, error) {
	rows := make([]statusQueryResult, 0)
	err := GetDatabase().NewRaw("select name, status, count(*) from jobs group by name, status").Scan(context.Background(), &rows)
	if err != nil {
		return Status{}, err
	}
	result := Status{
		Counts: map[string]Count{},
	}
	for _, row := range rows {
		count := result.Counts[row.Name]
		switch row.Status {
		case "running":
			count.Running = row.Count
		case "pending":
			count.Pending = row.Count
		case "failed":
			count.Failed = row.Count
		}

		result.Counts[row.Name] = count
	}

	return result, nil
}

/*
Unregister all the workers so postgres doesn't try to send jobs to these workers that are shutting down
*/
func (manager Manager) shutdown() {
	_, err := GetDatabase().NewDelete().Model(&RegisterQueue{}).Where("id = any(?)", pgdialect.Array(manager.queues)).Exec(context.Background())
	if err != nil {
		logger.Fatal(err)
	}
}

func (manager Manager) ping() {
	_, err := GetDatabase().NewUpdate().Model(&RegisterQueue{}).Set("updated_at = now()").Where("id = any(?)", pgdialect.Array(manager.queues)).Exec(context.Background())
	if err != nil {
		logger.Fatal(err)
	}
}

var manager = Manager{
	onSuccess:             make(chan JobSuccess, 100),
	onError:               make(chan JobError, 100),
	onErrHandlerChan:      make(chan func(error error, job JobForEvent), 100),
	onSuccessHandlerChan:  make(chan func(job JobForEvent), 100),
	successHandlers:       make([]func(job JobForEvent), 0),
	errorHandlers:         make([]func(error error, job JobForEvent), 0),
	onShutdown:            make(chan bool),
	onShutdownHandlerChan: make(chan func(), 100),
	onQueueRegisteredChan: make(chan string, 100),
	queues:                make([]string, 0),
}
