package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-job-queue/internal/db"
	"distributed-job-queue/internal/job"
)

func main() {
	d, err := db.Open("jobs.db")
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	repo := job.NewRepository(d)
	svc := job.NewService(repo)
	// worker without HTTP publisher (workers don't broadcast to SSE directly)
	w := job.NewWorker(svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx.Done())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down worker...")
	cancel()
	// brief wait
	time.Sleep(500 * time.Millisecond)
}
