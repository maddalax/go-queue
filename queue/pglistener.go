package queue

import (
	"context"
	"github.com/uptrace/bun/driver/pgdriver"
)

type PgNotify struct {
	poller Poller
}

func createNotify(poller Poller) PgNotify {
	return PgNotify{
		poller: poller,
	}
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
					PERFORM pg_notify(concat('jobs:changed'), NEW.name);
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
		ln := pgdriver.NewListener(GetDatabase())
		if err := ln.Listen(context.Background(), "jobs:changed"); err != nil {
			panic(err)
		}
		for range ln.Channel() {
			if err != nil {
				continue
			}
			// Let the poller know there is a new pending job
			n.poller.channel <- true
		}
	}()

	return nil
}
