package queue

import (
	"context"
	"encoding/json"
	"github.com/uptrace/bun"
	"time"
)

type JobForEvent struct {
	Id        string
	Name      string
	Payload   json.RawMessage
	CreatedAt string
}

func toJobForEvent[T any](job Job[T]) JobForEvent {
	payload, _ := json.Marshal(job.Payload)
	return JobForEvent{
		Id:        job.Id,
		Name:      job.Name,
		Payload:   payload,
		CreatedAt: job.CreatedAt,
	}
}

type JobError struct {
	error error
	job   JobForEvent
}

type JobSuccess struct {
	job JobForEvent
}

type RawJob struct {
	bun.BaseModel `bun:"table:jobs"`
	Id            string
	Name          string
	Payload       json.RawMessage `bun:"type:jsonb"`
	CreatedAt     string          `bun:"type:timestamp"`
	Tries         int
	Status        string
	LockedBy      string
	LastPing      string
	Error         string
}

type Job[T any] struct {
	Id        string
	Name      string
	Payload   T
	CreatedAt string
	Tries     int
	Status    string
	LockedBy  string
}

func (job Job[T]) Complete() {
	_, err := GetDatabase().NewDelete().Model(&RawJob{}).Where("id = ?", job.Id).Exec(context.Background())
	if err != nil {
		manager.onError <- JobError{
			error: err,
			job:   toJobForEvent(job),
		}
	} else {
		manager.onSuccess <- JobSuccess{job: toJobForEvent(job)}
	}
}

func (job Job[T]) Fail(error error) {
	_, err := GetDatabase().NewUpdate().Model(&RawJob{}).Set("status = ?, locked_by = ?, error = ?", "failed", nil, error.Error()).Where("id = ?", job.Id).Exec(context.Background())
	if err != nil {
		manager.onError <- JobError{
			error: err,
			job:   toJobForEvent(job),
		}
	} else {
		manager.onError <- JobError{
			error: error,
			job:   toJobForEvent(job),
		}
	}
}

// Ping
//
//	Updates the table to let the Queue know that the job is still being processed.
//	The job should be updated every 30s. If 30s has gone by and the job has not been updated,
//	we can assume it is dead and need to be re-run
func (job Job[T]) Ping() {
	_, err := GetDatabase().NewUpdate().Model(&RawJob{}).Set("last_ping = ?", time.Now()).Where("id = ?", job.Id).Exec(context.Background())
	if err != nil {
		manager.onError <- JobError{
			error: err,
			job:   toJobForEvent(job),
		}
	}
}
