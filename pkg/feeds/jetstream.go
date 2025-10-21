package feeds

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	"github.com/bluesky-social/jetstream/pkg/models"
)

// Broadcaster manages subscribers to Jetstream events
type Broadcaster struct {
	listeners []chan *models.Event
	mu        sync.Mutex
}

// Subscribe returns a new channel that will receive Jetstream events
func (b *Broadcaster) Subscribe() chan *models.Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan *models.Event, 10)
	b.listeners = append(b.listeners, ch)
	return ch
}

// Broadcast sends an event to all subscribers without blocking
func (b *Broadcaster) Broadcast(event *models.Event) {
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
		// Log event details for debugging
		if evt.Commit != nil {
			slog.Debug("jetstream event",
				"did", evt.Did,
				"kind", evt.Kind,
				"operation", evt.Commit.Operation,
				"collection", evt.Commit.Collection,
			)
		}

		broadcaster.Broadcast(evt)
		return nil
	}

	// Use the sequential scheduler
	scheduler := sequential.NewScheduler("jetstream", slog.Default(), handleEvent)

	config := client.DefaultClientConfig()
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
// It will automatically reconnect on errors via the underlying client
func (jc *JetstreamClient) Start(ctx context.Context) error {
	slog.Info("starting jetstream client")

	// Start with no cursor (from beginning)
	// For production, you'd want to persist and restore this
	return jc.client.ConnectAndRead(ctx, nil)
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
