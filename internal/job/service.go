package job

import (
	"encoding/json"
	"errors"
	"log"
	"time"
)

// Service encapsulates business logic. In Option B it also persists events
// so that multiple API/worker instances can replay events to dashboards.
type Service struct {
	repo          *Repository
	maxConcurrent int
	maxPerMin     int
}

func NewService(r *Repository) *Service {
	return &Service{repo: r, maxConcurrent: 5, maxPerMin: 10}
}

func (s *Service) Submit(tenant, idemp, payload string) (*Job, error) {
	// quota checks
	c, err := s.repo.CountRunningByTenant(tenant)
	if err != nil {
		return nil, err
	}
	if c >= s.maxConcurrent {
		return nil, errors.New("max concurrent jobs reached")
	}
	c2, err := s.repo.CountCreatedInLastMin(tenant)
	if err != nil {
		return nil, err
	}
	if c2 >= s.maxPerMin {
		return nil, errors.New("rate limit exceeded")
	}

	j := &Job{TenantID: tenant, IdempotencyKey: idemp, Payload: payload}
	created, err := s.repo.CreateIfNotExist(j)
	if err != nil {
		return nil, err
	}

	// persist event (Option B)
	s.publishEvent("submitted", created)

	return created, nil
}

func (s *Service) LeaseNext(leaseSec int) (*Job, error) {
	j, err := s.repo.LeaseNext(leaseSec)
	if err != nil {
		return nil, err
	}
	if j != nil {
		s.publishEvent("started", j)
	}
	return j, nil
}

func (s *Service) Ack(id string) error {
	if err := s.repo.MarkDone(id); err != nil {
		return err
	}
	// load job to publish
	if j, err := s.repo.GetByID(id); err == nil {
		s.publishEvent("done", j)
	}
	return nil
}

func (s *Service) FailOrRetry(id string) (bool, error) {
	movedToDLQ, err := s.repo.MarkFailedOrRetry(id)
	if err != nil {
		return false, err
	}
	if j, err := s.repo.GetByID(id); err == nil {
		if movedToDLQ {
			s.publishEvent("failed_dlq", j)
		} else {
			s.publishEvent("retry", j)
		}
	}
	return movedToDLQ, nil
}

func (s *Service) RecoverExpiredLeasesLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			ids, _ := s.repo.RequeueExpiredLeases()
			for _, id := range ids {
				if j, err := s.repo.GetByID(id); err == nil {
					s.publishEvent("timeout", j)
				}
			}
		}
	}
}

func (s *Service) publishEvent(eventType string, j *Job) {
	// persist to events table
	if j == nil {
		return
	}
	payloadObj := map[string]interface{}{"id": j.ID, "tenant": j.TenantID, "status": j.Status}
	b, _ := json.Marshal(payloadObj)
	if err := s.repo.SaveEvent(j.ID, eventType, string(b)); err != nil {
		log.Printf("ERROR: Failed to save event: %v", err)
	}
}

func (s *Service) RecentEvents(limit int) ([]Event, error) {
	return s.repo.GetRecentEvents(limit)
}

func (s *Service) GetEventsAfterID(lastID int64) ([]Event, error) {
	return s.repo.GetEventsAfterID(lastID)
}

func (s *Service) Metrics() (map[string]int, error) {
	return s.repo.Counts()
}

func (s *Service) GetJob(id string) (*Job, error) {
	return s.repo.GetByID(id)
}
