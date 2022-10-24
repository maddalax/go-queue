package queue

import "context"

type Manager struct {
	started              bool
	onError              chan JobError
	onSuccess            chan JobSuccess
	onErrHandlerChan     chan func(error error, job JobForEvent)
	onSuccessHandlerChan chan func(job JobForEvent)
	successHandlers      []func(job JobForEvent)
	errorHandlers        []func(error error, job JobForEvent)
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
	go func() {
		for {
			select {
			case handler := <-manager.onSuccessHandlerChan:
				manager.successHandlers = append(manager.successHandlers, handler)
				break
			case handler := <-manager.onErrHandlerChan:
				manager.errorHandlers = append(manager.errorHandlers, handler)
				break
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

var manager = Manager{
	onSuccess:            make(chan JobSuccess, 100),
	onError:              make(chan JobError, 100),
	onErrHandlerChan:     make(chan func(error error, job JobForEvent), 100),
	onSuccessHandlerChan: make(chan func(job JobForEvent), 100),
	successHandlers:      make([]func(job JobForEvent), 0),
	errorHandlers:        make([]func(error error, job JobForEvent), 0),
}
