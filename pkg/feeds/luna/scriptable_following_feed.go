package luna

import (
	"bytes"
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
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/bluesky-social/indigo/repo"
	"github.com/gorilla/websocket"
	"github.com/samber/lo"

	"github.com/arnodel/golua/lib/base"
	"github.com/arnodel/golua/lib/mathlib"
	"github.com/arnodel/golua/lib/packagelib"
	"github.com/arnodel/golua/lib/stringlib"
	"github.com/arnodel/golua/lib/tablelib"
	"github.com/arnodel/golua/lib/utf8lib"
	rt "github.com/arnodel/golua/runtime"

	_ "github.com/mattn/go-sqlite3"
)

type ScriptableFollowingFeed struct {
	FeedActorDID           string
	FeedName               string
	DatabasePath           string
	db                     *sql.DB
	relayAddress           string
	appviewUrl             string
	runtimes               map[uint64]ScriptRuntime
	reportChannel          chan int
	restartFirehoseChannel chan bool
}

type ScriptRuntime struct {
	hash       uint64
	rt         *rt.Runtime
	chunk      *rt.Closure
	scriptSpec rt.Value
	filterFunc rt.Value
	cleanups   []func()
}

func Compile(script Script) (ScriptRuntime, error) {
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
		return ScriptRuntime{}, err
	}
	sr.chunk = chunk
	scriptSpec, err := rt.Call1(sr.rt.MainThread(), rt.FunctionValue(chunk))
	if err != nil {
		return ScriptRuntime{}, err
	}
	sr.scriptSpec = scriptSpec
	sr.filterFunc = scriptSpec.AsTable().Get(rt.StringValue("filter"))
	return sr, nil
}

func (sr *ScriptRuntime) Cleanup() {
	for _, cleanup := range sr.cleanups {
		if cleanup != nil {
			cleanup()
		}
	}
	sr.rt.MainThread().CollectGarbage()
	sr.rt = nil
	sr.chunk = nil
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

func (ff *ScriptableFollowingFeed) getFollowing(userDID string) ([]string, error) {
	rows, err := ff.db.Query(`
		SELECT to_did
		FROM follow_relationships
		WHERE from_did = ?`,
		userDID)
	if err != nil {
		return nil, err
	}

	dids := make([]string, 0)
	for rows.Next() {
		var did string
		err := rows.Scan(&did)
		if err != nil {
			return nil, err
		}
		dids = append(dids, did)
	}
	return dids, nil
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

	var maxIndex uint = math.MaxUint
	posts := make([]*appbsky.FeedDefs_SkeletonFeedPost, 0)
	for rows.Next() {
		var atPath string
		var index uint
		if err := rows.Scan(&atPath, &index); err != nil {
			slog.Error("error scanning row", slog.Any("err", err))
			continue
		}
		if index < maxIndex {
			maxIndex = index
		}
		fmt.Println(atPath, index, maxIndex)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: atPath,
		})
	}

	newCursor := fmt.Sprintf("%d", maxIndex)

	return posts, lo.ToPtr(newCursor), nil
}

func (ff *ScriptableFollowingFeed) Spawn(ctx context.Context) {
	db, err := sql.Open("sqlite3", ff.DatabasePath)
	if err != nil {
		log.Fatal(err)
	}
	//	defer db.Close()

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
		script text
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

	CREATE TABLE IF NOT EXISTS firehose_sync_position (
		cursor int
	) STRICT;
	CREATE INDEX IF NOT EXISTS firehose_sync_position_cursor_index ON firehose_sync_position (cursor);
	`)
	if err != nil {
		panic(err)
	}
	ff.db = db
	go ff.main(ctx)
	go ff.scrapeFollowers(ctx)
	go ff.runReports()
}

func (ff *ScriptableFollowingFeed) main(ctx context.Context) {
	errorChannel := make(chan error, 1)
	exitChannel := make(chan bool, 1)
	for {
		go ff.firehoseConsumer(ctx, errorChannel, exitChannel)
		select {
		case err := <-errorChannel:
			slog.Error("error in firehose consumer, restarting", slog.Any("err", err))
		case <-ff.restartFirehoseChannel:
			slog.Error("restart requested")
			exitChannel <- true
		}
		slog.Info("sleeping for 3 seconds before restart")
		time.Sleep(3 * time.Second)
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
				log.Printf("assuming connection went to shit", badIncomingCounter)
				ff.restartFirehoseChannel <- true
			}
			slog.Info("report", slog.Int("processed", int(counters.processed)), slog.Int("allowed", int(counters.allowed)), slog.Int("incoming", int(counters.incoming)))
			counters = Counters{}
		}
	}
}

func (ff *ScriptableFollowingFeed) scrapeNewAccounts(ctx context.Context) error {
	rows, err := ff.db.Query("SELECT from_did FROM scrape_state WHERE state = 'pending'")
	if err != nil {
		return fmt.Errorf("error querying scrape state: %w", err)
	}
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
func (ff *ScriptableFollowingFeed) scrapeFollowers(ctx context.Context) {
	for {
		err := ff.scrapeNewAccounts(ctx)
		if err != nil {
			slog.Error("error in follower scraper", slog.Any("err", err))
		}
		time.Sleep(10 * time.Second)
	}
}

type Script struct {
	Slot int64
	Text string
}

func (s Script) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(s.Text))
	return h.Sum64()
}

func (ff *ScriptableFollowingFeed) firehoseConsumer(ctx context.Context, errorChannel chan error, exitChannel chan bool) {
	var syncCursorDb *int64
	err := ff.db.QueryRow(`SELECT max(cursor) FROM firehose_sync_position`).Scan(&syncCursorDb)
	if err != nil {
		errorChannel <- err
		return
	}
	if syncCursorDb == nil {
		syncCursorDb = lo.ToPtr(int64(0))
	}
	syncCursor := *syncCursorDb
	var uri string
	if syncCursor != 0 {
		uri = fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", ff.relayAddress, syncCursor)
	} else {
		uri = fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos", ff.relayAddress)
	}
	con, _, err := websocket.DefaultDialer.Dial(uri, http.Header{})
	if err != nil {
		errorChannel <- err
		return
	}
	defer con.Close()
	defer func() {
		_, err := ff.db.Exec("INSERT INTO firehose_sync_position (cursor) VALUES (?)", syncCursor)
		if err != nil {
			panic(err)
		}
	}()
	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {

			// sync the cursor every 2000 events (approximately every second or couple of seconds)
			if (evt.Seq - syncCursor) > 2000 {
				_, err := ff.db.Exec("INSERT INTO firehose_sync_position (cursor) VALUES (?)", syncCursor)
				if err != nil {
					slog.Error("error inserting cursor to firehose_sync_position", slog.String("err", err.Error()))
				}
				syncCursor = evt.Seq
			}
			rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
			if err != nil {
				return nil
			}
			userDid := evt.Repo

			for _, op := range evt.Ops {
				slog.Debug("incoming event", slog.String("path", op.Path), slog.Any("cid", op.Cid), slog.String("action", op.Action), slog.String("repo", evt.Repo))
				rcid, recBytes, err := rr.GetRecordBytes(ctx, op.Path)
				if err != nil {
					continue
				}
				slog.Debug("event", slog.String("rcid", rcid.String()))

				recordType, recordData, err := data.ExtractTypeCBORReader(bytes.NewReader(*recBytes))
				if err != nil {
					continue
				}
				slog.Debug("record", slog.String("record", recordType))

				switch recordType {
				case "app.bsky.graph.follow":
					// only follows from users we scraped shall be synced
					var state string
					row := ff.db.QueryRow("SELECT state FROM scrape_state WHERE from_did = ?", userDid)
					err = row.Scan(&state)
					if errors.Is(err, sql.ErrNoRows) {
						slog.Debug("no scrape state found for user, ignoring", slog.String("user_did", userDid))
						continue
					}
					if state != "ready" {
						continue
					}

					rec, err := data.UnmarshalCBOR(recordData)
					if err != nil {
						continue
					}

					recJSON, err := json.Marshal(rec)
					if err != nil {
						continue
					}

					subject, ok := rec["subject"].(string)
					slog.Debug("follow", slog.String("text", string(recJSON)))
					if ok {
						_, err := ff.db.Exec(`INSERT INTO follow_relationships (from_did, to_did) VALUES ($1, $2) ON CONFLICT DO NOTHING`, userDid, subject)
						if err != nil {
							slog.Error("error inserting following", slog.Any("err", err))
						} else {
							slog.Debug("followed", slog.String("from", userDid), slog.String("to", subject))
						}
					}
				case "app.bsky.feed.post":
					rec, err := data.UnmarshalCBOR(recordData)
					if err != nil {
						continue
					}
					ff.reportChannel <- INCOMING_POST

					atPath := fmt.Sprintf("at://%s/%s", userDid, op.Path)
					ok, err := ff.handlePost(rec, atPath)
					if err != nil {
						slog.Error("error handling post", slog.String("path", atPath), slog.Any("err", err))
						continue
					}
					// if none of the scripts allowed the post, we don't need to store it
					ff.reportChannel <- PROCESSED_POST
					if !ok {
						continue
					}
					ff.reportChannel <- ALLOWED_POST

					row := ff.db.QueryRow(`SELECT MAX(counter) FROM posts`)
					var maybeCurrentMaxIndex *uint64
					err = row.Scan(&maybeCurrentMaxIndex)
					if err != nil {
						slog.Error("error getting max index", slog.Any("err", err))
						continue
					}

					var newIndex uint64
					if maybeCurrentMaxIndex != nil {
						newIndex = *maybeCurrentMaxIndex + 1
					}
					_, err = ff.db.Exec(`INSERT INTO posts (author_did, at_path, counter) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, userDid, atPath, newIndex)
					if err != nil {
						slog.Error("error inserting post", slog.Any("err", err))
					} else {
						slog.Debug("post created", slog.String("at", atPath))
					}
				default:
					slog.Debug("unknown record type, ignoring", slog.String("record", recordType))
				}
			}

			return nil
		},
	}

	sched := sequential.NewScheduler("scriptable_following_feed", rsc.EventHandler)
	defer sched.Shutdown()

	go func() {
		err = events.HandleRepoStream(context.Background(), con, sched)
		errorChannel <- err
		exitChannel <- true
	}()

	<-exitChannel
}

func recToTable(anyV any) rt.Value {
	switch v := anyV.(type) {
	case nil:
		return rt.NilValue
	case bool:
		return rt.BoolValue(v)
	case int64:
		return rt.IntValue(v)
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
	case data.Blob:
		return recToTable(map[string]any{
			"mimeType": v.MimeType,
			"size":     v.Size,
			"ref":      v.Ref,
		})
	case data.CIDLink:
		return recToTable(v.String())
	default:
		slog.Warn("unknown value", slog.Any("v", anyV), slog.String("type", reflect.TypeOf(anyV).String()))
		return rt.NilValue
	}
}

func (ff ScriptableFollowingFeed) handlePost(record map[string]any, atPath string) (bool, error) {
	// we need to run every script for every user we know, and add to posts table for each script that allowed the post
	rows, err := ff.db.Query("SELECT from_did FROM scrape_state WHERE state = 'ready'")
	if err != nil {
		slog.Error("error querying scrape state", slog.Any("err", err))
		return false, err
	}
	usedRuntimes := make([]uint64, 0)
	var hadAnyAllowed bool
	recAsTable := recToTable(record)
	for rows.Next() {
		var fromDid string
		err := rows.Scan(&fromDid)
		if err != nil {
			slog.Error("error scanning scrape state did", slog.Any("err", err))
			continue
		}

		rows, err := ff.db.Query(`SELECT slot, script FROM scripts WHERE from_did = $1`, fromDid)
		if err != nil {
			slog.Error("error querying script rows from did", slog.Any("err", err), slog.String("from_did", fromDid))
			continue
		}

		followsTable := rt.NewTable()
		followsRows, err := ff.db.Query("SELECT to_did FROM follow_relationships WHERE from_did = ?", fromDid)
		if err != nil {
			slog.Error("error querying follows rows from did", slog.Any("err", err), slog.String("from_did", fromDid))
			continue
		}
		for followsRows.Next() {
			var followingDid string
			err = followsRows.Scan(&followingDid)
			if err != nil {
				slog.Error("error querying follow row from did", slog.Any("err", err), slog.String("from_did", fromDid))
				continue
			}
			followsTable.Set(rt.StringValue(followingDid), rt.IntValue(1))
		}
		followedTable := rt.NewTable()
		followedRows, err := ff.db.Query("SELECT from_did FROM follow_relationships WHERE to_did = ?", fromDid)
		if err != nil {
			slog.Error("error querying follows rows from did", slog.Any("err", err), slog.String("from_did", fromDid))
			continue
		}
		for followedRows.Next() {
			var followingDid string
			err = followsRows.Scan(&followingDid)
			if err != nil {
				slog.Error("error querying follow row from did", slog.Any("err", err), slog.String("from_did", fromDid))
				continue
			}
			followedTable.Set(rt.StringValue(followingDid), rt.IntValue(1))
		}

		for rows.Next() {
			var script Script
			err = rows.Scan(&script.Slot, &script.Text)
			if err != nil {
				slog.Error("error querying script row from did", slog.Any("err", err), slog.String("from_did", fromDid))
				continue
			}

			runtime, found := ff.runtimes[script.Hash()]
			if !found {
				newRuntime, err := Compile(script)
				if err != nil {
					slog.Error("error compiling script", slog.Any("err", err), slog.String("from_did", fromDid), slog.Int64("slot", script.Slot))
					continue
				}
				ff.runtimes[script.Hash()] = newRuntime
				runtime = newRuntime
			}
			usedRuntimes = append(usedRuntimes, runtime.hash)

			// NOTE: this gives the overall post context to the script
			t := rt.NewTable()
			t.Set(rt.StringValue("post"), recAsTable)
			// TODO optimize if a script doesn't request the follower/follow lists (e.g word scripts)
			t.Set(rt.StringValue("follows"), rt.TableValue(followsTable))
			t.Set(rt.StringValue("followed"), rt.TableValue(followedTable))
			runtime.rt.PushContext(rt.RuntimeContextDef{
				HardLimits: rt.RuntimeResources{
					Memory: 100000,
					Cpu:    1000000,
					Millis: 300,
				},
				RequiredFlags: rt.ComplyIoSafe | rt.ComplyCpuSafe | rt.ComplyMemSafe | rt.ComplyTimeSafe,
			})
			allowed, err := rt.Call1(runtime.rt.MainThread(), runtime.filterFunc, rt.TableValue(t))
			_ = runtime.rt.PopContext()
			if err != nil {
				slog.Error("error calling script", slog.Any("err", err), slog.String("from_did", fromDid))
				continue
			}
			isAllowed := allowed.AsBool()
			if isAllowed {
				hadAnyAllowed = true
				_, err = ff.db.Exec(`INSERT INTO allowed_posts (from_did, slot, at_path) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`, fromDid, script.Slot, atPath)
				if err != nil {
					slog.Error("error inserting allowed post", slog.Any("err", err))
				} else {
					slog.Debug("allowed post created", slog.String("at", atPath), slog.String("from", fromDid))
				}
			}
		}
	}

	for k, runtime := range ff.runtimes {
		if !slices.Contains(usedRuntimes, k) {
			slog.Warn("runtime not used", slog.Uint64("hash", k))
			delete(ff.runtimes, runtime.hash)
			runtime.Cleanup()
		}
	}

	return hadAnyAllowed, nil
}
