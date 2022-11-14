package main

import (
	"fmt"
	"go-queue/example/jobs"
	"go-queue/queue"
)

func main() {

	manager, err := queue.Initialize()

	if err != nil {
		panic(err)
	}
	//
	//for i := 0; i < 5000; i++ {
	//	queue.CreateWithHandler[jobs.SendEmailPayload](func(payload jobs.SendEmailPayload) error {
	//		println(fmt.Sprintf("sending email %s", payload.Email))
	//		time.Sleep(time.Second * 1)
	//		return nil
	//	})
	//}

	manager.OnJobSuccess(func(job queue.JobForEvent) {
		println(job.Id)
		//println(job.Payload)
	})

	manager.OnJobError(func(error error, job queue.JobForEvent) {
		println(error.Error())
	})

	for i := 0; i < 1000; i++ {
		jobs.SendEmail.Enqueue(jobs.SendEmailPayload{
			Email: "jm@madev.me",
			Body:  fmt.Sprintf("job queue: %d", i),
		})
	}

	//go func() {
	//	for {
	//		for i := 0; i < 30; i++ {
	//			jobs.SendEmail.Enqueue(jobs.SendEmailPayload{
	//				Email: "jm@madev.me",
	//				Body:  fmt.Sprintf("job queue: %d", i),
	//			})
	//		}
	//		time.Sleep(time.Second * 3)
	//	}
	//}()

	//go func() {
	//	for {
	//		println(fmt.Sprintf("total goroutines: %d", runtime.NumGoroutine()))
	//		time.Sleep(time.Second * 2)
	//	}
	//}()

	//go func() {
	//	for {
	//		counts, err := manager.Status()
	//		if err != nil {
	//			println(err.Error())
	//		} else {
	//			serialized, _ := json.Marshal(counts)
	//			println(string(serialized))
	//		}
	//		time.Sleep(time.Second * 1)
	//	}
	//}()

	end := make(chan struct{})
	go func() {
		manager.OnShutdown(func() {
			close(end)
		})
	}()
	<-end
}
