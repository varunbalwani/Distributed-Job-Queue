package main

import (
	"log"
	"net/http"
	"time"

	"distributed-job-queue/internal/db"
	"distributed-job-queue/internal/job"
	"distributed-job-queue/internal/server"
)

func main() {
	d, err := db.Open("jobs.db")
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	repo := job.NewRepository(d)
	svc := job.NewService(repo)
	srv := server.NewServer(svc)

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
