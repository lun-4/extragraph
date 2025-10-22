package feeds

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	"github.com/bluesky-social/jetstream/pkg/models"
)

// Broadcaster manages subscribers to Jetstream events
type Broadcaster struct {
	listeners    []chan *models.Event
	mu           sync.Mutex
	lastActivity atomic.Int64 // Unix timestamp in nanoseconds
}

// Subscribe returns a new channel that will receive Jetstream events
func (b *Broadcaster) Subscribe() chan *models.Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Larger buffer to handle firehose volume (can be 1000+ events/sec)
	ch := make(chan *models.Event, 10000)
	b.listeners = append(b.listeners, ch)
	return ch
}

// Broadcast sends an event to all subscribers without blocking
func (b *Broadcaster) Broadcast(event *models.Event) {
	// Update last activity time (atomic, no lock needed)
	b.lastActivity.Store(time.Now().UnixNano())

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.listeners {
		select {
		case ch <- event:
			// Event sent successfully
		default:
			// Channel full, skip to avoid blocking
			slog.Warn("jetstream broadcast: channel full, dropping event")
		}
	}
}

// GetLastActivity returns the time of the last event broadcast
func (b *Broadcaster) GetLastActivity() time.Time {
	nanos := b.lastActivity.Load()
	if nanos == 0 {
		return time.Time{} // Zero value
	}
	return time.Unix(0, nanos)
}

// JetstreamClient connects to Bluesky Jetstream and broadcasts events
type JetstreamClient struct {
	client      *client.Client
	broadcaster *Broadcaster
}

// NewJetstreamClient creates a new Jetstream client
func NewJetstreamClient() *JetstreamClient {
	broadcaster := &Broadcaster{}

	// Handler function that broadcasts events
	handleEvent := func(ctx context.Context, evt *models.Event) error {
		// Only broadcast events we care about to reduce load
		if evt.Kind == "commit" && evt.Commit != nil {
			slog.Debug("jetstream event",
				"did", evt.Did,
				"kind", evt.Kind,
				"operation", evt.Commit.Operation,
				"collection", evt.Commit.Collection,
			)
			broadcaster.Broadcast(evt)
		}
		return nil
	}

	// Use the sequential scheduler
	scheduler := sequential.NewScheduler("jetstream", slog.Default(), handleEvent)

	config := client.DefaultClientConfig()
	config.WantedCollections = []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.graph.follow"}
	config.WebsocketURL = "wss://jetstream1.us-east.bsky.network/subscribe"

	jetstreamClient, err := client.NewClient(config, slog.Default(), scheduler)
	if err != nil {
		slog.Error("failed to create jetstream client", "err", err)
		return nil
	}

	return &JetstreamClient{
		client:      jetstreamClient,
		broadcaster: broadcaster,
	}
}

// Start begins consuming from the Jetstream and broadcasting events
// It will automatically reconnect on errors with exponential backoff
func (jc *JetstreamClient) Start(ctx context.Context) {
	slog.Info("starting jetstream client")

	for {
		select {
		case <-ctx.Done():
			slog.Info("jetstream client shutting down")
			return
		default:
			// Reset lastActivity before each connection attempt to give the new connection
			// a grace period. Otherwise, we'd immediately kill new connections based on
			// stale timestamps from the previous connection.
			jc.broadcaster.lastActivity.Store(0)

			// Create a cancellable context for this connection attempt
			connCtx, cancelConn := context.WithCancel(ctx)

			// Start a watchdog goroutine to detect stuck connections
			// If we don't receive ANY events for 10 seconds, the connection is dead
			watchdogDone := make(chan struct{})
			go func() {
				ticker := time.NewTicker(1 * time.Second)
				defer ticker.Stop()
				defer close(watchdogDone)

				for {
					select {
					case <-connCtx.Done():
						return
					case <-ticker.C:
						lastActivity := jc.broadcaster.GetLastActivity()
						if !lastActivity.IsZero() && time.Since(lastActivity) > 10*time.Second {
							slog.Warn("jetstream watchdog: no events received in 10 seconds, reconnecting")
							cancelConn()
							return
						}
					}
				}
			}()

			// Start with no cursor (from beginning)
			// For production, you'd want to persist and restore this
			err := jc.client.ConnectAndRead(connCtx, nil)
			cancelConn()   // Cancel the watchdog
			<-watchdogDone // Wait for watchdog to exit

			if err != nil {
				if ctx.Err() != nil {
					// Main context cancelled, exit
					slog.Info("jetstream client shutting down")
					return
				}
				slog.Error("jetstream connection error", "err", err)
			}

			slog.Info("jetstream connection closed, reconnecting in 3 seconds")
			time.Sleep(3 * time.Second)
		}
	}
}

// Subscribe returns a channel that receives Jetstream events
func (jc *JetstreamClient) Subscribe() chan *models.Event {
	return jc.broadcaster.Subscribe()
}

// DebugPrintEvent is a helper to pretty-print events for debugging
func DebugPrintEvent(event *models.Event) {
	data, _ := json.MarshalIndent(event, "", "  ")
	slog.Debug("jetstream event", "event", string(data))
}
