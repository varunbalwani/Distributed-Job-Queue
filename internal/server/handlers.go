package server

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"distributed-job-queue/internal/job"

	"github.com/gorilla/mux"
)

// SSE broadcaster simple implementation
type Broadcaster struct {
	clients map[chan string]struct{}
}

func NewBroadcaster() *Broadcaster { return &Broadcaster{clients: map[chan string]struct{}{}} }

func (b *Broadcaster) Register(c chan string) {
	b.clients[c] = struct{}{}
}
func (b *Broadcaster) Unregister(c chan string) { delete(b.clients, c) }
func (b *Broadcaster) Publish(msg string) {
	for c := range b.clients {
		select {
		case c <- msg:
		default:
		}
	}
}

// server struct

type Server struct {
	svc *job.Service
	b   *Broadcaster
}

func NewServer(svc *job.Service) *Server {
	s := &Server{svc: svc, b: NewBroadcaster()}
	var lastID int64
	// preload recent events into broadcaster so new UIs can see history slightly
	// (Moved to handleEvents for per-client replay)
	if evs, err := svc.RecentEvents(1); err == nil && len(evs) > 0 {
		if evs[0].ID > lastID {
			lastID = evs[0].ID
		}
	}
	go s.pollEvents(lastID)
	return s
}

func (s *Server) pollEvents(lastID int64) {
	log.Printf("Started polling events from ID %d", lastID)
	for {
		time.Sleep(1 * time.Second)
		evs, err := s.svc.GetEventsAfterID(lastID)
		if err != nil {
			log.Printf("Error polling events: %v", err)
			continue
		}
		for _, e := range evs {
			if e.ID > lastID {
				lastID = e.ID
			}
			// broadcast
			msg := `{"event":"` + e.Type + `","job_id":"` + e.JobID + `","payload":` + jsonEscape(e.Payload) + `}`
			s.b.Publish(msg)
		}
	}
}

func jsonEscape(in string) string {
	if len(in) > 0 && (in[0] == '{' || in[0] == '[' || in[0] == '"') {
		return in
	}
	b, _ := json.Marshal(in)
	return string(b)
}

func (s *Server) Router() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/jobs", s.handleSubmit).Methods("POST")
	r.HandleFunc("/jobs/{id}", s.handleGet).Methods("GET")
	r.HandleFunc("/dashboard", s.handleDashboard).Methods("GET")
	r.HandleFunc("/events", s.handleEvents)
	r.HandleFunc("/metrics", s.handleMetrics).Methods("GET")
	r.PathPrefix("/").Handler(http.FileServer(http.Dir("ui")))
	return r
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Payload        json.RawMessage `json:"payload"`
		IdempotencyKey string          `json:"idempotency_key"`
		Tenant         string          `json:"tenant"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if in.Tenant == "" {
		in.Tenant = "demo"
	}
	j, err := s.svc.Submit(in.Tenant, in.IdempotencyKey, string(in.Payload))
	if err != nil {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	j, err := s.svc.GetJob(id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "ui/index.html")
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	c := make(chan string, 50)
	s.b.Register(c)
	defer s.b.Unregister(c)

	ctx := r.Context()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// send initial ping
	w.Write([]byte("event: ping\ndata: \n\n"))
	flusher.Flush()

	// Ping worker to wake it up if needed
	go s.PingWorker()

	// Replay recent events for this client
	if evs, err := s.svc.RecentEvents(50); err == nil {
		for i := len(evs) - 1; i >= 0; i-- {
			e := evs[i]
			msg := `{"event":"replay","job_id":"` + e.JobID + `","type":"` + e.Type + `","payload":` + jsonEscape(e.Payload) + `}`
			w.Write([]byte("event: replay\n"))
			w.Write([]byte("data: " + msg + "\n\n"))
		}
		flusher.Flush()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c:
			w.Write([]byte("event: update\n"))
			w.Write([]byte("data: " + msg + "\n\n"))
			flusher.Flush()
		case <-time.After(30 * time.Second):
			// keep alive
			w.Write([]byte(":\n\n"))
			flusher.Flush()
		}
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m, err := s.svc.Metrics()
	if err != nil {
		http.Error(w, "error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m)
}

// PingWorker attempts to wake up the worker service by calling its health endpoint
func (s *Server) PingWorker() {
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
