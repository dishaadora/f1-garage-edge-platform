// Command ingest is the trackside telemetry ingest service.
//
// It listens for HTTP POST events (sent by replay/replay_session.py, or by
// a real telemetry feed later), buffers them in memory per (session,
// channel), and periodically flushes each buffer to a local Parquet file.
// This is Stage 1: local ingest -> local Parquet, queryable by DuckDB.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/parquet-go/parquet-go"
)

// event is the on-disk / in-memory shape of one telemetry sample.
//
// Fields are pointers so a sample that's missing a channel (a real
// possibility -- sensors drop readings) serializes as Parquet "null"
// instead of a misleading zero value. This mirrors the row_to_event()
// null-handling on the Python side.
type event struct {
	Driver             string   `parquet:"driver"`
	Source             string   `parquet:"source"`
	LapNumber          *int64   `parquet:"lap_number,optional"`
	SessionTimeSeconds float64  `parquet:"session_time_seconds"`
	Speed              *float64 `parquet:"speed,optional"`
	Throttle           *float64 `parquet:"throttle,optional"`
	Brake              *bool    `parquet:"brake,optional"`
	RPM                *float64 `parquet:"rpm,optional"`
	NGear              *float64 `parquet:"n_gear,optional"`
	DRS                *float64 `parquet:"drs,optional"`
	IngestedAtUnixMs   int64    `parquet:"ingested_at_unix_ms"`
}

// rawEvent mirrors the JSON body sent by row_to_event() in
// replay_session.py. Decoding into this first, then mapping into event,
// keeps "what the wire format looks like" separate from "what we store."
type rawEvent struct {
	Driver             string   `json:"driver"`
	Source             string   `json:"source"`
	LapNumber          *int64   `json:"lap_number"`
	SessionTimeSeconds float64  `json:"session_time_seconds"`
	Speed              *float64 `json:"Speed"`
	Throttle           *float64 `json:"Throttle"`
	Brake              *bool    `json:"Brake"`
	RPM                *float64 `json:"RPM"`
	NGear              *float64 `json:"nGear"`
	DRS                *float64 `json:"DRS"`
}

// buffer holds not-yet-flushed events for one channel (e.g. "car_data"),
// guarded by its own mutex so channels can be written to concurrently
// without contending on a single global lock.
type buffer struct {
	mu     sync.Mutex
	events []event
}

// server holds all buffers for the current session, keyed by channel
// (the "Source" field on each event -- "car_data", "pos_data", etc).
// This is the "partitioned by session and channel" structure: session
// is the server's own identity (one ingest process = one session), and
// channel is the map key.
type server struct {
	sessionID string
	dataDir   string

	buffersMu sync.Mutex
	buffers   map[string]*buffer
}

func newServer(sessionID, dataDir string) *server {
	return &server{
		sessionID: sessionID,
		dataDir:   dataDir,
		buffers:   make(map[string]*buffer),
	}
}

func (s *server) bufferFor(channel string) *buffer {
	s.buffersMu.Lock()
	defer s.buffersMu.Unlock()
	b, ok := s.buffers[channel]
	if !ok {
		b = &buffer{}
		s.buffers[channel] = b
	}
	return b
}

func (s *server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var raw rawEvent
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		// A malformed event is logged and rejected, but must not take
		// down the service -- the same "fail soft" principle as the
		// replay script's own send_event().
		log.Printf("ingest: rejecting malformed event: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if raw.Source == "" {
		http.Error(w, "missing source", http.StatusBadRequest)
		return
	}

	e := event{
		Driver:             raw.Driver,
		Source:             raw.Source,
		LapNumber:          raw.LapNumber,
		SessionTimeSeconds: raw.SessionTimeSeconds,
		Speed:              raw.Speed,
		Throttle:           raw.Throttle,
		Brake:              raw.Brake,
		RPM:                raw.RPM,
		NGear:              raw.NGear,
		DRS:                raw.DRS,
		IngestedAtUnixMs:   time.Now().UnixMilli(),
	}

	b := s.bufferFor(raw.Source)
	b.mu.Lock()
	b.events = append(b.events, e)
	b.mu.Unlock()

	w.WriteHeader(http.StatusAccepted)
}

// flushAll writes every channel's buffered events to a new Parquet file
// and clears the buffer. Called on a timer and once more on shutdown so
// the last partial buffer isn't silently dropped.
func (s *server) flushAll() {
	s.buffersMu.Lock()
	channels := make([]string, 0, len(s.buffers))
	for ch := range s.buffers {
		channels = append(channels, ch)
	}
	s.buffersMu.Unlock()

	for _, channel := range channels {
		s.flushChannel(channel)
	}
}

func (s *server) flushChannel(channel string) {
	b := s.bufferFor(channel)

	b.mu.Lock()
	if len(b.events) == 0 {
		b.mu.Unlock()
		return
	}
	// Swap out the buffer under lock, then release it immediately --
	// writing to disk should never block new events from arriving.
	toWrite := b.events
	b.events = nil
	b.mu.Unlock()

	dir := filepath.Join(s.dataDir, s.sessionID, channel)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("flush: could not create dir %s: %v", dir, err)
		return
	}

	filename := filepath.Join(dir, fmt.Sprintf("%d.parquet", time.Now().UnixNano()))
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("flush: could not create file %s: %v", filename, err)
		return
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[event](f)
	if _, err := writer.Write(toWrite); err != nil {
		log.Printf("flush: write failed for %s: %v", filename, err)
		return
	}
	if err := writer.Close(); err != nil {
		log.Printf("flush: close failed for %s: %v", filename, err)
		return
	}

	log.Printf("flush: wrote %d events to %s", len(toWrite), filename)
}

func main() {
	port := flag.Int("port", 8080, "HTTP port to listen on")
	sessionID := flag.String("session-id", "", "identifier for this session, e.g. 2024-monza-R (required)")
	dataDir := flag.String("data-dir", "./data", "root directory for Parquet output")
	flushInterval := flag.Duration("flush-interval", 10*time.Second, "how often to flush buffered events to disk")
	flag.Parse()

	if *sessionID == "" {
		log.Fatal("ingest: --session-id is required (e.g. --session-id 2024-monza-R)")
	}

	srv := newServer(*sessionID, *dataDir)

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", srv.handleIngest)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	// Flush on a fixed interval so data never sits unwritten (and
	// unqueryable) for longer than flushInterval.
	ticker := time.NewTicker(*flushInterval)
	go func() {
		for range ticker.C {
			srv.flushAll()
		}
	}()

	// Flush once more on shutdown (Ctrl+C / SIGTERM) so the last
	// partial buffer isn't lost -- important because a demo or a real
	// deploy will get killed mid-buffer far more often than it will
	// exit cleanly on its own.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("ingest: shutting down, flushing remaining buffers...")
		ticker.Stop()
		srv.flushAll()
		os.Exit(0)
	}()

	log.Printf("ingest: listening on :%d, session=%s, data-dir=%s, flush-interval=%s",
		*port, *sessionID, *dataDir, *flushInterval)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("ingest: server error: %v", err)
	}
}