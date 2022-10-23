package jobs

import (
	"fmt"
	"go-queue/queue"
	"time"
)

type SendEmailPayload struct {
	Email string
	Body  string
}

var SendEmail = queue.CreateWithHandler[SendEmailPayload](func(payload SendEmailPayload) error {
	println(fmt.Sprintf("sending email %s", payload.Email))
	time.Sleep(time.Second * 2)
	return nil
})
