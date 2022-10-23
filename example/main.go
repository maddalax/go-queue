package main

import (
	"go-queue/example/jobs"
	"go-queue/queue"
	"sync"
)

func main() {

	manager, err := queue.Initialize()

	if err != nil {
		panic(err)
	}

	manager.OnJobSuccess(func(job queue.JobForEvent) {
		println(job.Id)
		println(job.Payload)
	})

	manager.OnJobError(func(error error, job queue.JobForEvent) {
		println(error.Error())
	})

	jobs.SendEmail.Enqueue(jobs.SendEmailPayload{
		Email: "jm@madev.me",
		Body:  "job queue's are really cool",
	})

	// WaitGroup here so the process does not end
	var wg = &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
