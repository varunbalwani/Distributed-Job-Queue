package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"distributed-job-queue/internal/db"
	"distributed-job-queue/internal/job"
	"distributed-job-queue/internal/server"
)

// pingWorker attempts to wake up the worker service by calling its health endpoint
func pingWorker() {
	workerURL := os.Getenv("WORKER_URL")
	if workerURL == "" {
		workerURL = "http://localhost:10000/health"
	}

	client := &http.Client{Timeout: 5 * time.Second}

	log.Printf("Pinging worker service at %s...", workerURL)
	resp, err := client.Get(workerURL)
	if err != nil {
		log.Printf("Warning: Failed to ping worker service: %v", err)
		log.Println("Make sure the worker service is running with: go run ./cmd/worker/main.go")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Println("Worker service is alive and responding")
	} else {
		log.Printf("Worker service responded with status: %d", resp.StatusCode)
	}
}

func main() {
	// PostgreSQL connection string from environment variable or default for local dev
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		connString = "postgresql://jobqueue:jobqueue_dev@localhost:5432/jobqueue?sslmode=disable"
		log.Println("DATABASE_URL not set, using default:", connString)
	}

	d, err := db.Open(connString)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	repo := job.NewRepository(d)
	svc := job.NewService(repo)
	srv := server.NewServer(svc)

	// Ping worker service to ensure it's running
	go pingWorker()

	httpServer := &http.Server{
		Addr:         ":8080",
		Handler:      srv.Router(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	log.Println("API listening :8080")
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}
