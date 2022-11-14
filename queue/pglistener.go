package queue

import (
	"context"
	"encoding/json"
	"github.com/uptrace/bun/driver/pgdriver"
)

type EventPayload struct {
	Job    RawJob
	Worker string
}

type NotifyHandler struct {
	id      string
	event   string
	handler func(job RawJob)
}

type PgNotify struct {
	handlers    map[string]func(job RawJob)
	changeChan  chan string
	handlerChan chan NotifyHandler
}

func createNotify() PgNotify {
	return PgNotify{
		handlers:    make(map[string]func(job RawJob)),
		changeChan:  make(chan string, 100),
		handlerChan: make(chan NotifyHandler, 100),
	}
}

func (n PgNotify) addHandler(handler NotifyHandler) {
	n.handlerChan <- handler
}

func setup() error {
	_, err := GetDatabase().NewCreateTable().Model(&RawJob{}).Table("jobs").IfNotExists().Exec(context.Background())
	if err != nil {
		return err
	}

	query := `
		BEGIN;

CREATE OR REPLACE FUNCTION job_change_function()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.status = 'pending' THEN
        PERFORM pg_notify(concat('jobs:changed'), json_build_object('job', NEW, 'worker',
                                                                    (SELECT id from workers w WHERE w.name = NEW.name ORDER BY random() LIMIT 1))::text);
    END IF;
    RETURN NULL;
END;
$$
    LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS job_create_trigger ON jobs;
DROP TRIGGER IF EXISTS job_update_trigger ON jobs;

CREATE TRIGGER job_update_trigger
    AFTER UPDATE
    ON jobs
    FOR EACH ROW
EXECUTE PROCEDURE job_change_function();

CREATE TRIGGER job_create_trigger
    AFTER INSERT
    ON jobs
    FOR EACH ROW
EXECUTE PROCEDURE job_change_function();

COMMIT;
	`

	_, err = GetDatabase().Exec(query)

	if err != nil {
		return err
	}

	return nil
}

func (n PgNotify) Start() error {
	err := setup()
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case handler := <-n.handlerChan:
				n.handlers[handler.id] = handler.handler
				break
			case message := <-n.changeChan:
				payload := EventPayload{}
				err := json.Unmarshal([]byte(message), &payload)
				if err != nil {
					logger.Fatal(err)
				}
				if worker, ok := n.handlers[payload.Worker]; ok {
					// This may block if all the processors for this worker are full
					worker(payload.Job)
				}
			}
		}
	}()

	go func() {
		ln := pgdriver.NewListener(GetDatabase())
		if err := ln.Listen(context.Background(), "jobs:changed"); err != nil {
			panic(err)
		}
		for notif := range ln.Channel() {
			n.changeChan <- notif.Payload
		}
	}()

	return nil
}
