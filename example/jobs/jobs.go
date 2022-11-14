package jobs

import (
	"encoding/json"
	"fmt"
	"go-queue/queue"
	"log"
	"os"
	"time"
)

type SendEmailPayload struct {
	Email string
	Body  string
}

type SendTextPayload struct {
	Email string
	Body  string
}

var SendEmail = queue.CreateWithHandler[SendEmailPayload](func(payload SendEmailPayload) error {
	time.Sleep(time.Second * 2)
	f, err := os.OpenFile("events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	logger := log.New(f, fmt.Sprintf("%T", payload)+" ", log.LstdFlags)
	serialized, err := json.Marshal(payload)
	logger.Println(string(serialized))
	return nil
})

var SendText = queue.CreateWithHandler[SendTextPayload](func(payload SendTextPayload) error {
	time.Sleep(time.Second * 2)
	f, err := os.OpenFile("events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	logger := log.New(f, fmt.Sprintf("%T", payload)+" ", log.LstdFlags)
	serialized, err := json.Marshal(payload)
	logger.Println(string(serialized))
	return nil
})
