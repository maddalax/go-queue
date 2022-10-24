package queue

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"os"
	"time"
)

// channelBuffer The total amount of messages the job change listener can hold into memory at once. These messages are
// just a notification to tell a worker to query for any new jobs, it does not contain any job data.
// This is useful because if we know that we have 2500 jobs to run, send these 2500 messages to the workers to tell them
// to query for new jobs 2500 times, therefore running all jobs without having for them to manually poll./*
const channelBuffer = 10000

// The amount of jobs we would like to query at once.
const prefetchLimit = 250

type PollerHandler struct {
	event   string
	handler chan RawJob
}

type Poller struct {
	id          string
	channel     chan bool
	handlers    map[string]chan RawJob
	handlerChan chan PollerHandler
}

func createPoller() Poller {
	hostname, _ := os.Hostname()
	return Poller{
		// id that is used to set the lockedBy column on the job table, so we know which process is handling which jobs
		// TODO change hostname to something like machineId -> https://github.com/denisbrodbeck/machineid
		id:          fmt.Sprintf(hostname),
		channel:     make(chan bool, channelBuffer),
		handlers:    make(map[string]chan RawJob, 0),
		handlerChan: make(chan PollerHandler, 10),
	}
}

// Start the poller and the listener for our pg_notify listener channel
func (poller Poller) start() {
	go poller.poll()
	go poller.listen()
}

// Poll the jobs table every minute to check the count to see if there are any jobs
// we need to process. This is useful for if messages get missed / dropped from the
// pg_notify call.
func (poller Poller) poll() {
	for {
		count, err := GetDatabase().NewSelect().Model(&RawJob{}).Where("status = ?", "pending").Count(context.Background())
		if err != nil {
			println(err.Error())
			// TODO logging
			continue
		}

		// Drain the channel to match count
		if len(poller.channel) > count {
			for len(poller.channel) > count {
				<-poller.channel
			}
		}

		diff := count - len(poller.channel)
		// Ensure we don't send more than the max limit of the channel
		if diff > channelBuffer {
			diff = channelBuffer
		}
		for i := 0; i < diff; i++ {
			poller.channel <- true
		}
		time.Sleep(time.Minute)
	}
}

// Listen for messages from the pg_notify call.
// Since it would be pretty inefficient to query postgres for a single job every single time
// a pg_notify message comes in, we batch these up and query postgres with the amount of pg_notify
// messages we received in N (prefetchTimeout) time or when we reach prefetchLimit. Whichever comes first.
// This allows us to query postgres with a limit of X instead of 1. Such as 100 or 250.
func (poller Poller) listen() {
	prefetchTimeout := 5 * time.Second
	messages := 0
	tick := time.NewTicker(prefetchTimeout)

	handlers := make(map[string]chan RawJob, 0)

	for {
		select {

		case handler := <-poller.handlerChan:
			handlers[handler.event] = handler.handler

		// Check if a new messages is available.
		// If so, store it and check if the messages
		// has exceeded its size limit.
		case _ = <-poller.channel:
			messages += 1

			if messages < prefetchLimit {
				break
			}

			// Reset the timeout ticker.
			// Otherwise we will get too many sends.
			tick.Stop()

			// Send the cached messages and reset the messages.
			poller.sendNextJobs(messages, handlers)
			messages = 0

			// Recreate the ticker, so the timeout trigger
			// remains consistent.
			tick = time.NewTicker(prefetchTimeout)

		// If the timeout is reached, send the
		// current message messages, regardless of
		// its size.
		case <-tick.C:
			poller.sendNextJobs(messages, handlers)
			messages = 0
		}
	}
}

// Adds a new handler for a specific event.
// The poller will call back to this handler when it queries a new job for this specific event type
func (poller Poller) addHandler(handler PollerHandler) {
	poller.handlerChan <- handler
}

// Query this next available jobs in our jobs table

// The process goes:
// Query the jobs within a transaction and lock the rows
// Update the status of these jobs to 'running' and locked_by to whatever our process name is.
// Since we are querying more than one job at a time, we can't process them within this single transaction 1 by 1
// It would be too slow and there would be many issues if a job failed.
// So because of this, run each job in a goroutine separately and have them handle failures themselves.

// When a processor picks up the job, they will 'ping' the jobs table (updating locked_by) every 30s to ensure
// that they are still running that job. If the locked_by does not get updated, that job is declared dead and
// set back to pending (if retries are enabled), or set to failed.

// There is also a prefetch limit that we want to abide by, to ensure we aren't processing too many jobs at once
// on a single system, so we can only be running at maximum whatever the prefetchLimit is set to.
// Example:
//
//		Prefetch limit: 250
//	 	Currently processing: 150
//	 	We can only query at maximum 100 more jobs to process
func (poller Poller) sendNextJobs(count int, handlers map[string]chan RawJob) {

	if count > prefetchLimit {
		count = prefetchLimit
	}

	if count == 0 {
		count = prefetchLimit
	}

	err := GetDatabase().RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.Exec("BEGIN")
		if err != nil {
			return err
		}

		inProgress, err := tx.NewSelect().Model(&RawJob{}).Where("locked_by = ? and status = ?", poller.id, "running").Count(context.Background())

		if err != nil {
			return err
		}

		diff := prefetchLimit - inProgress

		if count > diff {
			count = diff
		}

		if count <= 0 {
			count = 0
		}

		var jobs []RawJob

		err = tx.NewRaw(`
		UPDATE jobs
		SET status = 'running', locked_by = ?
		FROM (SELECT * FROM jobs WHERE status = 'pending' LIMIT ? FOR UPDATE SKIP LOCKED) AS subquery
		WHERE jobs.id = subquery.id
		RETURNING subquery.*;
		`, poller.id, diff).Scan(ctx, &jobs)

		notSent := make([]string, 0)
		for _, job := range jobs {
			handler := handlers[job.Name]
			select {
			case handler <- job:
				break
			default:
				// If the handlers (the workers) are backed up because they are processing too many jobs, set the jobs back
				// to pending
				notSent = append(notSent, job.Id)
			}
		}

		if len(notSent) > 0 {
			logger.Println(fmt.Sprintf("setting %d jobs back to pending because they were unable to send to processor", len(notSent)))
			_, err := tx.NewUpdate().Model(&RawJob{}).Where("id = any(?)", pgdialect.Array(notSent)).Set("status = 'pending', locked_by = null").Exec(context.Background())
			if err != nil {
				return err
			}
		}

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		println(err.Error())
	}
}
