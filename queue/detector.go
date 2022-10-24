package queue

import (
	"context"
	"time"
)

func startFailureDetector() {
	go func() {
		for {
			// If the job hasn't been pinged in a full minute, assume it is dead and needs to be set back to pending to be picked up again
			// TODO should this set failure instead? maybe based on retries?
			err := GetDatabase().NewRaw("UPDATE jobs SET status = 'pending', locked_by = null, last_ping = now() WHERE last_ping < now() - interval '1 minute' RETURNING true").Scan(context.Background(), &struct{}{})
			if err != nil {
				logger.Println(err.Error())
			}
			time.Sleep(time.Minute)
		}
	}()
}
