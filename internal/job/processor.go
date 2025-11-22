package job

import (
	"errors"
	"log"
	"math/rand"
	"time"
)

// Processor simulates processing of a job. Replace with real logic.
type Processor struct{}

func NewProcessor() *Processor { return &Processor{} }

func (p *Processor) Process(j *Job) error {
	// simulate work
	time.Sleep(500 * time.Millisecond)
	// randomly fail ~20%
	if rand.Intn(100) < 20 {
		return errors.New("simulated failure")
	}
	return nil
}

// Worker coordinates leasing and processing

type Worker struct {
	svc  *Service
	proc *Processor
}

func NewWorker(s *Service) *Worker {
	return &Worker{svc: s, proc: NewProcessor()}
}

func (w *Worker) Start(ctxDone <-chan struct{}) {
	stop := make(chan struct{})
	go w.svc.RecoverExpiredLeasesLoop(stop)

	leaseSec := 5
	log.Println("Worker started, polling for jobs...")
	for {
		select {
		case <-ctxDone:
			close(stop)
			return
		default:
		}
		j, err := w.svc.LeaseNext(leaseSec)
		if err != nil {
			log.Printf("Error leasing job: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if j == nil {
			// log.Println("No jobs found") // verbose
			time.Sleep(200 * time.Millisecond)
			continue
		}

		log.Printf("Leased job %s", j.ID)
		// process synchronously to keep things simple
		if err := w.proc.Process(j); err != nil {
			log.Printf("Job %s failed: %v", j.ID, err)
			_, _ = w.svc.FailOrRetry(j.ID)
			continue
		}
		log.Printf("Job %s completed", j.ID)
		_ = w.svc.Ack(j.ID)
	}
}
