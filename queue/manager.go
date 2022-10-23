package queue

type Manager struct {
	onError              chan JobError
	onSuccess            chan JobSuccess
	onErrHandlerChan     chan func(error error, job JobForEvent)
	onSuccessHandlerChan chan func(job JobForEvent)
	successHandlers      []func(job JobForEvent)
	errorHandlers        []func(error error, job JobForEvent)
}

func (manager Manager) Start() {

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

var manager = Manager{
	onSuccess:            make(chan JobSuccess, 100),
	onError:              make(chan JobError, 100),
	onErrHandlerChan:     make(chan func(error error, job JobForEvent), 100),
	onSuccessHandlerChan: make(chan func(job JobForEvent), 100),
	successHandlers:      make([]func(job JobForEvent), 0),
	errorHandlers:        make([]func(error error, job JobForEvent), 0),
}
