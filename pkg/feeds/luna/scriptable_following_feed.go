package luna

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/samber/lo"

	"github.com/ericvolp12/go-bsky-feed-generator/pkg/feeds"

	"github.com/arnodel/golua/lib/base"
	"github.com/arnodel/golua/lib/mathlib"
	"github.com/arnodel/golua/lib/packagelib"
	"github.com/arnodel/golua/lib/stringlib"
	"github.com/arnodel/golua/lib/tablelib"
	"github.com/arnodel/golua/lib/utf8lib"
	rt "github.com/arnodel/golua/runtime"

	extism "github.com/extism/go-sdk"

	_ "github.com/mattn/go-sqlite3"
)

type LockedInt struct {
	mut sync.Mutex
	ui  uint
}

func (li *LockedInt) Incr() {
	li.Lock()
	defer li.Unlock()
	li.ui++
}

func (li *LockedInt) Lock() {
	li.mut.Lock()
}
func (li *LockedInt) Unlock() {
	li.mut.Unlock()
}
func (li *LockedInt) UnlockedGet() uint {
	return li.ui
}

func (li *LockedInt) LockAndGet() uint {
	li.Lock()
	defer li.Unlock()
	return li.ui
}
func (li *LockedInt) LockAndSet(v uint) uint {
	li.Lock()
	defer li.Unlock()
	li.ui = v
	return li.ui
}

func (li *LockedInt) Reset() uint {
	li.Lock()
	defer li.Unlock()
	val := li.ui
	li.ui = 0
	return val
}

type ScriptableFollowingFeed struct {
	FeedActorDID           string
	FeedName               string
	DatabasePath           string
	db                     *sql.DB
	relayAddress           string
	appviewUrl             string
	runtimes               map[uint64]Runtime
	reportChannel          chan int
	restartFirehoseChannel chan bool
	syncCursor             LockedInt
	jetstreamClient        *feeds.JetstreamClient
}

// Runtime is a common interface for both Lua and WASM script runtimes
type Runtime interface {
	Hash() uint64
	Cleanup()
}

type ScriptRuntime struct {
	hash       uint64
	rt         *rt.Runtime
	chunk      *rt.Closure
	scriptSpec rt.Value
	filterFunc rt.Value
	cleanups   []func()
}

type ExtismRuntime struct {
	hash   uint64
	plugin *extism.Plugin
}

func Compile(script Script) (*ScriptRuntime, error) {
	sr := ScriptRuntime{hash: script.Hash(), cleanups: make([]func(), 0)}
	sr.rt = rt.New(os.Stdout)
	base.Load(sr.rt)
	sr.cleanups = append(sr.cleanups, packagelib.LibLoader.Run(sr.rt))
	sr.cleanups = append(sr.cleanups, stringlib.LibLoader.Run(sr.rt))
	sr.cleanups = append(sr.cleanups, mathlib.LibLoader.Run(sr.rt))
	sr.cleanups = append(sr.cleanups, tablelib.LibLoader.Run(sr.rt))
	sr.cleanups = append(sr.cleanups, utf8lib.LibLoader.Run(sr.rt))
	chunk, err := sr.rt.CompileAndLoadLuaChunk("test", []byte(script.Text), rt.TableValue(sr.rt.GlobalEnv()))
	if err != nil {
		return nil, err
	}
	sr.chunk = chunk
	scriptSpec, err := rt.Call1(sr.rt.MainThread(), rt.FunctionValue(chunk))
	if err != nil {
		return nil, err
	}
	sr.scriptSpec = scriptSpec
	sr.filterFunc = scriptSpec.AsTable().Get(rt.StringValue("filter"))
	return &sr, nil
}

func (sr *ScriptRuntime) Hash() uint64 {
	return sr.hash
}

func (sr *ScriptRuntime) Cleanup() {
	// Clear references first
	sr.chunk = nil
	sr.scriptSpec = rt.NilValue
	sr.filterFunc = rt.NilValue

	for _, cleanup := range sr.cleanups {
		if cleanup != nil {
			cleanup()
		}
	}
	sr.rt.MainThread().CollectGarbage()
	sr.rt = nil
	sr.chunk = nil
	sr.cleanups = nil // Clear the cleanup slice
}

func CompileExtism(ctx context.Context, script Script) (*ExtismRuntime, error) {
	if script.Type != "wasm" {
		return nil, fmt.Errorf("script type must be 'wasm', got '%s'", script.Type)
	}

	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmData{Data: script.WasmBytecode},
		},
		Memory: &extism.ManifestMemory{
			MaxPages: 32, // 32 pages = 2MB (WASM modules typically need ~16 pages minimum)
		},
		Timeout: 300, // 300 milliseconds
	}

	config := extism.PluginConfig{
		EnableWasi: false, // Disable WASI for security
	}

	plugin, err := extism.NewPlugin(ctx, manifest, config, []extism.HostFunction{})
	if err != nil {
		return nil, fmt.Errorf("failed to create Extism plugin: %w", err)
	}

	return &ExtismRuntime{
		hash:   script.Hash(),
		plugin: plugin,
	}, nil
}

func (er *ExtismRuntime) Hash() uint64 {
	return er.hash
}

func (er *ExtismRuntime) Cleanup() {
	if er.plugin != nil {
		// Close with background context since we're just cleaning up
		er.plugin.Close(context.Background())
		er.plugin = nil
	}
}

func (ff *ScriptableFollowingFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	feeds := make([]appbsky.FeedDescribeFeedGenerator_Feed, 0)
	for i := range 5 {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: fmt.Sprintf("at://"+ff.FeedActorDID+"/app.bsky.feed.generator/%s_%d", ff.FeedName, i+1),
		})
	}
	return feeds, nil
}

func (ff *ScriptableFollowingFeed) GetFeedNames() []string {
	feeds := make([]string, 0)
	for i := range 5 {
		feeds = append(feeds, fmt.Sprintf("%s_%d", ff.FeedName, i+1))
	}
	return feeds
}

func (ff *ScriptableFollowingFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	slog.Info("following feed page", slog.String("feed", feed), slog.String("user", userDID), slog.Int64("limit", limit), slog.String("cursor", cursor))

	splitted := strings.Split(feed, "_")
	feedName := splitted[0]
	if feedName != ff.FeedName {
		return nil, nil, fmt.Errorf("unknown feed name: want %s, got %s", ff.FeedName, feed)
	}

	slot, err := strconv.ParseInt(splitted[1], 10, 32)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid slot number: %s", splitted[1])
	}
	if slot < 1 && slot > 5 {
		return nil, nil, fmt.Errorf("unknown slot number: %d", slot)
	}

	var cursorAsIndex uint = math.MaxInt64 - 1
	if cursor != "" {
		cursorAsIndexParsed, err := strconv.ParseUint(cursor, 10, 32)
		if err != nil {
			slog.Error("cursor invalid", slog.String("cursor", cursor), slog.Any("err", err))
			return nil, nil, err
		}
		cursorAsIndex = uint(cursorAsIndexParsed)
	}

	query := `
		SELECT posts.at_path, posts.counter
		FROM allowed_posts
		JOIN posts
			ON allowed_posts.at_path = posts.at_path
		WHERE
			posts.counter < ?
		AND allowed_posts.from_did = ?
		AND allowed_posts.slot = ?
		ORDER BY counter DESC`

	query += fmt.Sprintf(" LIMIT %d", limit)

	rows, err := ff.db.Query(query, cursorAsIndex, userDID, slot)
	if err != nil {
		slog.Error("error getting posts", slog.String("user", userDID), slog.Any("err", err))
		return nil, nil, err
	}
	defer rows.Close()

	var minIndex uint = math.MaxUint
	posts := make([]*appbsky.FeedDefs_SkeletonFeedPost, 0)
	for rows.Next() {
		var atPath string
		var index uint
		if err := rows.Scan(&atPath, &index); err != nil {
			slog.Error("error scanning row", slog.Any("err", err))
			continue
		}
		if index < minIndex {
			minIndex = index
		}
		fmt.Println(atPath, index, minIndex)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: atPath,
		})
	}

	newCursor := fmt.Sprintf("%d", minIndex)

	return posts, lo.ToPtr(newCursor), nil
}

func (ff *ScriptableFollowingFeed) Spawn(ctx context.Context) {
	db, err := sql.Open("sqlite3", ff.DatabasePath)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
	PRAGMA journal_mode=WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	PRAGMA cache_size = 1000000000;
	PRAGMA foreign_keys = true;
	PRAGMA temp_store = memory;

	CREATE TABLE IF NOT EXISTS follow_relationships (
		from_did text,
		to_did text,
		primary key (from_did, to_did)
	) STRICT;
	CREATE INDEX IF NOT EXISTS follow_relationships_from_did_index ON follow_relationships (from_did);

	CREATE TABLE IF NOT EXISTS scrape_state (
		from_did text primary key,
		state text
	) STRICT;

	CREATE TABLE IF NOT EXISTS scripts (
		from_did text primary key,
		slot int,
		script text,
		script_type text DEFAULT 'lua',
		wasm_bytecode blob
	) STRICT;

	CREATE TABLE IF NOT EXISTS posts (
		author_did text,
		at_path text,
		counter int unique,
		primary key (author_did, at_path)
	) STRICT;
	CREATE INDEX IF NOT EXISTS posts_author_did_index ON posts (author_did);
	CREATE INDEX IF NOT EXISTS posts_at_path_index ON posts (at_path);

	CREATE TABLE IF NOT EXISTS allowed_posts (
		from_did text,
		slot int,
		at_path text,
		primary key (from_did, slot, at_path)
	) STRICT;
	CREATE INDEX IF NOT EXISTS allowed_posts_from_did_index ON allowed_posts (from_did);
	CREATE INDEX IF NOT EXISTS allowed_posts_slot_index ON allowed_posts (slot);
	`)
	if err != nil {
		panic(err)
	}
	ff.db = db
	go ff.main(ctx)
	go ff.scrapeFollowers()
	go ff.runReports()
}

func (ff *ScriptableFollowingFeed) main(ctx context.Context) {
	defer ff.db.Close()

	if ff.jetstreamClient == nil {
		slog.Error("jetstream client is nil, cannot start scriptable feed")
		return
	}

	// Subscribe to jetstream events
	eventsChan := ff.jetstreamClient.Subscribe()
	slog.Info("scriptable feed subscribed to jetstream")

	for {
		select {
		case <-ctx.Done():
			slog.Info("scriptable feed shutting down")
			return
		case <-ff.restartFirehoseChannel:
			slog.Info("restart requested, but using shared jetstream client")
		case evt := <-eventsChan:
			if evt == nil {
				continue
			}
			ff.handleJetstreamEvent(ctx, evt)
		}
	}
}

// handleJetstreamEvent processes a single Jetstream event
func (ff *ScriptableFollowingFeed) handleJetstreamEvent(ctx context.Context, evt *models.Event) {
	if evt.Kind != "commit" || evt.Commit == nil {
		return
	}

	commit := evt.Commit
	userDid := evt.Did

	slog.Debug("jetstream event",
		"did", userDid,
		"operation", commit.Operation,
		"collection", commit.Collection,
		"rkey", commit.RKey,
	)

	switch commit.Collection {
	case "app.bsky.graph.follow":
		ff.handleFollow(userDid, commit)
	case "app.bsky.feed.post":
		ff.handlePostFromJetstream(ctx, userDid, commit)
	case "app.bsky.feed.repost":
		ff.handleRepostFromJetstream(ctx, userDid, commit)
	default:
		slog.Debug("unhandled collection", "collection", commit.Collection)
	}
}

// handleFollow processes a follow event from Jetstream
func (ff *ScriptableFollowingFeed) handleFollow(userDid string, commit *models.Commit) {
	// Skip delete operations
	if commit.Operation == "delete" {
		return
	}

	// only follows from users we scraped shall be synced
	var state string
	row := ff.db.QueryRow("SELECT state FROM scrape_state WHERE from_did = ?", userDid)
	err := row.Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		slog.Debug("no scrape state found for user, ignoring", "user_did", userDid)
		return
	}
	if state != "ready" {
		return
	}

	// Parse the record JSON (skip if empty)
	if len(commit.Record) == 0 {
		slog.Debug("empty record for follow", "did", userDid)
		return
	}

	var rec map[string]interface{}
	err = json.Unmarshal(commit.Record, &rec)
	if err != nil {
		slog.Debug("error unmarshaling follow record", "err", err, "did", userDid)
		return
	}

	subject, ok := rec["subject"].(string)
	if !ok {
		slog.Debug("follow record missing subject")
		return
	}

	_, err = ff.db.Exec(`INSERT INTO follow_relationships (from_did, to_did) VALUES ($1, $2) ON CONFLICT DO NOTHING`, userDid, subject)
	if err != nil {
		slog.Error("error inserting following", "err", err)
	} else {
		slog.Debug("followed", "from", userDid, "to", subject)
	}
}

// handlePostFromJetstream processes a post event from Jetstream
func (ff *ScriptableFollowingFeed) handlePostFromJetstream(ctx context.Context, userDid string, commit *models.Commit) {
	ff.handleRecordFromJetstream(ctx, userDid, commit, "post", ff.handlePost)
}

func (ff *ScriptableFollowingFeed) handleRepostFromJetstream(ctx context.Context, userDid string, commit *models.Commit) {
	ff.handleRecordFromJetstream(ctx, userDid, commit, "repost", ff.handleRepost)
}

// handleRecordFromJetstream is a generic handler for post/repost records from Jetstream
func (ff *ScriptableFollowingFeed) handleRecordFromJetstream(
	ctx context.Context,
	userDid string,
	commit *models.Commit,
	recordType string,
	handler func(string, map[string]any, string) (bool, error),
) {
	_ = ctx
	// Skip delete/update operations - we only care about creates
	if commit.Operation != "create" {
		return
	}

	// Skip if record is empty
	if len(commit.Record) == 0 {
		slog.Debug("empty record", "type", recordType, "did", userDid, "rkey", commit.RKey)
		return
	}

	// Parse the record JSON
	var rec map[string]any
	err := json.Unmarshal(commit.Record, &rec)
	if err != nil {
		slog.Debug("error unmarshaling record", "type", recordType, "err", err, "did", userDid, "rkey", commit.RKey)
		return
	}

	ff.reportChannel <- INCOMING_POST

	atPath := fmt.Sprintf("at://%s/%s/%s", userDid, commit.Collection, commit.RKey)
	ok, err := handler(userDid, rec, atPath)
	if err != nil {
		slog.Error("error handling record", "type", recordType, "path", atPath, "err", err)
		return
	}

	ff.reportChannel <- PROCESSED_POST
	if !ok {
		return
	}
	ff.reportChannel <- ALLOWED_POST

	// Get max counter and insert record
	row := ff.db.QueryRow(`SELECT MAX(counter) FROM posts`)
	var maybeCurrentMaxIndex *uint64
	err = row.Scan(&maybeCurrentMaxIndex)
	if err != nil {
		slog.Error("error getting max index", "err", err)
		return
	}

	var newIndex uint64
	if maybeCurrentMaxIndex != nil {
		newIndex = *maybeCurrentMaxIndex + 1
	}

	_, err = ff.db.Exec(`INSERT INTO posts (author_did, at_path, counter) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, userDid, atPath, newIndex)
	if err != nil {
		slog.Error("error inserting record", "type", recordType, "err", err)
	} else {
		slog.Debug("record created", "type", recordType, "at", atPath)
	}
}

const (
	PROCESSED_POST int = 1
	ALLOWED_POST   int = 2
	INCOMING_POST  int = 3
)

type Counters struct {
	incoming  uint
	processed uint
	allowed   uint
}

func (ff *ScriptableFollowingFeed) runReports() {
	counters := Counters{}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	badIncomingCounter := 0

	for {
		select {
		case report := <-ff.reportChannel:
			switch report {
			case ALLOWED_POST:
				counters.allowed++
			case PROCESSED_POST:
				counters.processed++
			case INCOMING_POST:
				counters.incoming++
			}
		case <-ticker.C:
			if counters.processed == 0 {
				log.Printf("no events for %d amount of seconds", badIncomingCounter)
				badIncomingCounter++
			}
			if badIncomingCounter > 20 {
				log.Printf("assuming connection went to shit. we got %d seconds with zero events", badIncomingCounter)
				ff.restartFirehoseChannel <- true
			}
			slog.Info("report", slog.Int("processed", int(counters.processed)), slog.Int("allowed", int(counters.allowed)), slog.Int("incoming", int(counters.incoming)))
			counters = Counters{}
		}
	}
}

func (ff *ScriptableFollowingFeed) scrapeNewAccounts() error {
	rows, err := ff.db.Query("SELECT from_did FROM scrape_state WHERE state = 'pending'")
	if err != nil {
		return fmt.Errorf("error querying scrape state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var fromDid string
		err := rows.Scan(&fromDid)
		if err != nil {
			slog.Error("error scanning scrape state did", slog.Any("err", err))
			continue
		}
		slog.Info("scraping new did", slog.String("did", fromDid))
		var cursor string
		for {
			var url string
			if cursor != "" {
				url = fmt.Sprintf("%s/xrpc/app.bsky.graph.getFollows?actor=%s&cursor=%s", ff.appviewUrl, fromDid, cursor)
			} else {
				url = fmt.Sprintf("%s/xrpc/app.bsky.graph.getFollows?actor=%s", ff.appviewUrl, fromDid)
			}
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				return fmt.Errorf("error creating request: %w", err)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("error sending request: %w", err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				return fmt.Errorf("error sending request: status %d", res.StatusCode)
			}
			resBody, err := io.ReadAll(res.Body)
			if err != nil {
				return fmt.Errorf("error reading response body: %w", err)
			}
			var data map[string]any
			err = json.Unmarshal(resBody, &data)
			if err != nil {
				return fmt.Errorf("error unmarshaling response: %w", err)
			}

			for _, followAny := range data["follows"].([]any) {
				follow := followAny.(map[string]any)
				subject := follow["did"].(string)
				_, err := ff.db.Exec(`INSERT INTO follow_relationships (from_did, to_did) VALUES ($1, $2) ON CONFLICT DO NOTHING`, fromDid, subject)
				if err != nil {
					slog.Error("error inserting follow", slog.Any("err", err), slog.String("from", fromDid), slog.String("to", subject))
				} else {
					slog.Info("followed", slog.String("from", fromDid), slog.String("to", subject))
				}
			}
			if data["cursor"] == nil {
				break
			} else {
				cursor = data["cursor"].(string)
			}
		}
		_, err = ff.db.Exec("UPDATE scrape_state SET state = 'ready' WHERE from_did = ?", fromDid)
		if err != nil {
			return fmt.Errorf("error updating scrape state: %w", err)
		}
		slog.Info("scraped new followers", slog.String("from", fromDid))
	}
	return nil
}
func (ff *ScriptableFollowingFeed) scrapeFollowers() {
	for {
		err := ff.scrapeNewAccounts()
		if err != nil {
			slog.Error("error in follower scraper", slog.Any("err", err))
		}
		time.Sleep(10 * time.Second)
	}
}

type Script struct {
	Slot         int64
	Text         string
	Type         string // "lua" or "wasm"
	WasmBytecode []byte
}

func (s Script) Hash() uint64 {
	h := fnv.New64a()
	if s.Type == "wasm" {
		h.Write(s.WasmBytecode)
	} else {
		h.Write([]byte(s.Text))
	}
	return h.Sum64()
}

func recToTable(anyV any) rt.Value {
	switch v := anyV.(type) {
	case nil:
		return rt.NilValue
	case bool:
		return rt.BoolValue(v)
	case int64:
		return rt.IntValue(v)
	case float64:
		return rt.FloatValue(v)
	case string:
		return rt.StringValue(v)
	case []any:
		res := make([]rt.Value, 0)
		for _, inner := range v {
			res = append(res, recToTable(inner))
		}
		return rt.ArrayValue(res)
	case map[any]any:
		out := rt.NewTable()
		for anyK, anyV := range v {
			k := recToTable(anyK)
			v := recToTable(anyV)
			out.Set(k, v)
		}
		return rt.TableValue(out)
	case map[string]any:
		out := rt.NewTable()
		for anyK, anyV := range v {
			k := recToTable(anyK)
			v := recToTable(anyV)
			out.Set(k, v)
		}
		return rt.TableValue(out)
	case atdata.Blob:
		return recToTable(map[string]any{
			"mimeType": v.MimeType,
			"size":     v.Size,
			"ref":      v.Ref,
		})
	case atdata.CIDLink:
		return recToTable(v.String())
	default:
		slog.Warn("unknown value", slog.Any("v", anyV), slog.String("type", reflect.TypeOf(anyV).String()))
		return rt.NilValue
	}
}

// executeWasmFilter executes a WASM filter plugin with the given context
func (ff *ScriptableFollowingFeed) executeWasmFilter(
	ctx context.Context,
	runtime *ExtismRuntime,
	recordAuthorDid string,
	recordType string,
	record map[string]any,
	followsMap map[string]int,
	followedMap map[string]int,
) (bool, error) {
	_ = ctx
	slog.Debug("executeWasmFilter START")

	// Build context map for JSON serialization
	contextMap := make(map[string]any)
	contextMap["author_did"] = recordAuthorDid

	// Use the native Go record directly - no conversion needed!
	if recordType == "post" {
		contextMap["post"] = record
		contextMap["repost"] = nil
	} else {
		contextMap["post"] = nil
		contextMap["repost"] = record
	}

	// Use native Go maps directly - no conversion needed!
	slog.Debug("using follows map")
	contextMap["follows"] = followsMap
	slog.Debug("follows set")

	slog.Debug("using followed map")
	contextMap["followed"] = followedMap
	slog.Debug("followed set")

	// Serialize to JSON
	slog.Debug("marshaling to JSON")
	inputJSON, err := json.Marshal(contextMap)
	if err != nil {
		return false, fmt.Errorf("failed to marshal context: %w", err)
	}
	slog.Debug("JSON marshaled", slog.Int("bytes", len(inputJSON)))

	// Call the filter function in the WASM plugin
	slog.Debug("calling WASM plugin")
	exitCode, resultBytes, err := runtime.plugin.Call("filter", inputJSON)
	slog.Debug("WASM plugin returned", slog.Int("exitCode", int(exitCode)), slog.Any("err", err))
	if err != nil {
		return false, fmt.Errorf("failed to call filter function: %w", err)
	}

	if exitCode != 0 {
		return false, fmt.Errorf("filter function returned non-zero exit code: %d", exitCode)
	}

	// Parse the result (expecting a boolean JSON value)
	slog.Debug("unmarshaling result")
	var result bool
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal filter result: %w", err)
	}

	slog.Debug("executeWasmFilter DONE", slog.Bool("result", result))
	return result, nil
}

func (ff *ScriptableFollowingFeed) processScriptsForUser(
	ctx context.Context,
	fromDid string, recordAuthorDid string,
	atPath string, recordType string, record map[string]any, recAsTable rt.Value) (bool, []uint64, error) {
	slog.Debug("processScriptsForUser START", slog.String("from_did", fromDid))
	var usedRuntimes []uint64
	var hadAnyAllowed bool

	scriptRows, err := ff.db.Query(`SELECT slot, script, script_type, wasm_bytecode FROM scripts WHERE from_did = $1`, fromDid)
	slog.Debug("script query done", slog.Any("err", err))
	if err != nil {
		slog.Error("error querying script rows from did", slog.Any("err", err), slog.String("from_did", fromDid))
		return false, nil, err
	}
	defer scriptRows.Close()

	slog.Debug("querying follows")
	// Use Go map instead of Lua table - will convert to Lua table only when needed
	followsMap := make(map[string]int)
	followsRows, err := ff.db.Query("SELECT to_did FROM follow_relationships WHERE from_did = ?", fromDid)
	if err != nil {
		slog.Error("error querying follows rows from did", slog.Any("err", err), slog.String("from_did", fromDid))
		return false, nil, err
	}
	defer followsRows.Close()

	for followsRows.Next() {
		var followingDid string
		err = followsRows.Scan(&followingDid)
		if err != nil {
			slog.Error("error querying follow row from did", slog.Any("err", err), slog.String("from_did", fromDid))
			continue
		}
		followsMap[followingDid] = 1
	}
	slog.Debug("follows done")

	slog.Debug("querying followed")
	// Use Go map instead of Lua table - will convert to Lua table only when needed
	followedMap := make(map[string]int)
	followedRows, err := ff.db.Query("SELECT from_did FROM follow_relationships WHERE to_did = ?", fromDid)
	if err != nil {
		slog.Error("error querying follows rows from did", slog.Any("err", err), slog.String("from_did", fromDid))
		return false, nil, err
	}
	defer followedRows.Close()

	for followedRows.Next() {
		var followingDid string
		err = followedRows.Scan(&followingDid)
		if err != nil {
			slog.Error("error querying follow row from did", slog.Any("err", err), slog.String("from_did", fromDid))
			continue
		}
		followedMap[followingDid] = 1
	}
	slog.Debug("followed done")

	slog.Debug("starting script loop")
	for scriptRows.Next() {
		slog.Debug("processing script row")
		var script Script
		var scriptType sql.NullString
		var wasmBytecode []byte
		err = scriptRows.Scan(&script.Slot, &script.Text, &scriptType, &wasmBytecode)
		if err != nil {
			slog.Error("error querying script row from did", slog.Any("err", err), slog.String("from_did", fromDid))
			continue
		}
		slog.Debug("scanned script row", slog.Int64("slot", script.Slot))

		// Set script type (default to "lua" for backwards compatibility)
		if scriptType.Valid {
			script.Type = scriptType.String
		} else {
			script.Type = "lua"
		}
		script.WasmBytecode = wasmBytecode
		slog.Debug("script type set", slog.String("type", script.Type))

		runtime, found := ff.runtimes[script.Hash()]
		slog.Debug("runtime lookup", slog.Bool("found", found), slog.String("type", script.Type))
		if !found {
			var newRuntime Runtime
			var err error

			slog.Info("compiling new runtime", slog.String("type", script.Type), slog.String("user", fromDid))
			if script.Type == "wasm" {
				newRuntime, err = CompileExtism(ctx, script)
			} else {
				newRuntime, err = Compile(script)
			}
			slog.Debug("compilation done", slog.Any("err", err))

			if err != nil {
				slog.Error("error compiling script", slog.Any("err", err), slog.String("from_did", fromDid), slog.Int64("slot", script.Slot), slog.String("type", script.Type))
				continue
			}
			ff.runtimes[script.Hash()] = newRuntime
			runtime = newRuntime
		}
		usedRuntimes = append(usedRuntimes, runtime.Hash())

		// Execute filter based on runtime type
		var isAllowed bool

		slog.Debug("executing filter", slog.String("type", script.Type))
		switch r := runtime.(type) {
		case *ScriptRuntime:
			// Lua execution path - convert Go maps to Lua tables
			followsTable := rt.NewTable()
			for did := range followsMap {
				followsTable.Set(rt.StringValue(did), rt.IntValue(1))
			}
			followedTable := rt.NewTable()
			for did := range followedMap {
				followedTable.Set(rt.StringValue(did), rt.IntValue(1))
			}

			t := rt.NewTable()
			t.Set(rt.StringValue("author_did"), rt.StringValue(recordAuthorDid))
			if recordType == "post" {
				t.Set(rt.StringValue("post"), recAsTable)
				t.Set(rt.StringValue("repost"), rt.NilValue)
			} else {
				t.Set(rt.StringValue("post"), rt.NilValue)
				t.Set(rt.StringValue("repost"), recAsTable)
			}
			t.Set(rt.StringValue("follows"), rt.TableValue(followsTable))
			t.Set(rt.StringValue("followed"), rt.TableValue(followedTable))

			r.rt.PushContext(rt.RuntimeContextDef{
				HardLimits: rt.RuntimeResources{
					Memory: 100000,
					Cpu:    1000000,
					Millis: 300,
				},
				RequiredFlags: rt.ComplyIoSafe | rt.ComplyCpuSafe | rt.ComplyMemSafe | rt.ComplyTimeSafe,
			})
			allowed, err := rt.Call1(r.rt.MainThread(), r.filterFunc, rt.TableValue(t))
			_ = r.rt.PopContext()
			if err != nil {
				slog.Error("error calling lua script", slog.Any("err", err), slog.String("from_did", fromDid))
				continue
			}
			isAllowed = allowed.AsBool()

		case *ExtismRuntime:
			// WASM execution path - use native Go maps directly
			isAllowed, err = ff.executeWasmFilter(ctx, r, recordAuthorDid, recordType, record, followsMap, followedMap)
			if err != nil {
				slog.Error("error calling wasm script", slog.Any("err", err), slog.String("from_did", fromDid))
				continue
			}

		default:
			slog.Error("unknown runtime type", slog.String("from_did", fromDid))
			continue
		}
		if isAllowed {
			hadAnyAllowed = true
			_, err = ff.db.Exec(`INSERT INTO allowed_posts (from_did, slot, at_path) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`, fromDid, script.Slot, atPath)
			if err != nil {
				slog.Error("error inserting allowed post", slog.Any("err", err))
			} else {
				slog.Debug("allowed "+recordType+" created", slog.String("at", atPath), slog.String("from", fromDid))
			}
		}
	}

	return hadAnyAllowed, usedRuntimes, nil
}

func (ff *ScriptableFollowingFeed) handleRecord(recordAuthorDid string, record map[string]any, atPath string, recordType string) (bool, error) {
	slog.Debug("handleRecord called", slog.String("type", recordType), slog.String("author", recordAuthorDid))

	// we need to run every script for every user we know, and add to posts table for each script that allowed the record
	rows, err := ff.db.Query("SELECT from_did FROM scrape_state WHERE state = 'ready'")
	if err != nil {
		slog.Error("error querying scrape state", slog.Any("err", err))
		return false, err
	}
	defer rows.Close()

	allUsedRuntimes := make([]uint64, 0)
	var hadAnyAllowed bool

	slog.Debug("converting record to table")
	recAsTable := recToTable(record)
	slog.Debug("record converted to table")

	for rows.Next() {
		var fromDid string
		err := rows.Scan(&fromDid)
		if err != nil {
			slog.Error("error scanning scrape state did", slog.Any("err", err))
			continue
		}

		slog.Debug("processing scripts for user", slog.String("from_did", fromDid))
		userHadAllowed, usedRuntimes, err := ff.processScriptsForUser(context.Background(), fromDid, recordAuthorDid, atPath, recordType, record, recAsTable)
		slog.Debug("processScriptsForUser returned", slog.String("from_did", fromDid), slog.Bool("allowed", userHadAllowed), slog.Any("err", err))
		if err != nil {
			slog.Error("error processing scripts for user", slog.Any("err", err), slog.String("from_did", fromDid))
			continue
		}

		if userHadAllowed {
			slog.Info("allowed", slog.String("from_did", fromDid), slog.String("author_did", recordAuthorDid))
			hadAnyAllowed = true
		}
		allUsedRuntimes = append(allUsedRuntimes, usedRuntimes...)
	}

	for k, runtime := range ff.runtimes {
		if !slices.Contains(allUsedRuntimes, k) {
			slog.Warn("runtime not used", slog.Uint64("hash", k))
			delete(ff.runtimes, k)
			runtime.Cleanup()
		}
	}

	return hadAnyAllowed, nil
}

func (ff *ScriptableFollowingFeed) handlePost(recordAuthorDid string, record map[string]any, atPath string) (bool, error) {
	result, err := ff.handleRecord(recordAuthorDid, record, atPath, "post")
	return result, err
}

func (ff *ScriptableFollowingFeed) handleRepost(recordAuthorDid string, record map[string]any, atPath string) (bool, error) {
	result, err := ff.handleRecord(recordAuthorDid, record, atPath, "repost")
	return result, err
}
