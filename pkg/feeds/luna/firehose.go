package luna

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ericvolp12/go-bsky-feed-generator/pkg/feedrouter"
	"github.com/gorilla/websocket"
	"github.com/samber/lo"

	_ "github.com/mattn/go-sqlite3"
)

type DynamicFeed interface {
	feedrouter.Feed
	Spawn(context.Context)
	GetFeedNames() []string
}

func ConfigureLunaFeeds(ctx context.Context) ([]DynamicFeed, error) {
	relayAddress := os.Getenv("RELAY_WEBSOCKET_ADDRESS")
	if relayAddress == "" {
		panic("RELAY_WEBSOCKET_ADDRESS is required")
	}
	appviewUrl := os.Getenv("APPVIEW_URL")
	if appviewUrl == "" {
		panic("APPVIEW_URL is required")
	}
	feeds := make([]DynamicFeed, 0)
	if os.Getenv("FOLLOWING_FEED_ENABLE") == "1" {
		feeds = append(feeds, &FollowingFeed{
			FeedActorDID:     os.Getenv("FOLLOWING_FEED_ACTOR_DID"),
			FeedName:         os.Getenv("FOLLOWING_FEED_NAME"),
			DiscoverFeedName: os.Getenv("DISCOVER_FEED_NAME"),
			relayAddress:     relayAddress,
		})
	} else {
		slog.Warn("Following feed is disabled")
	}
	if os.Getenv("SCRIPTABLE_FOLLOWING_FEED_ENABLE") == "1" {
		databasePath := os.Getenv("SCRIPTABLE_FOLLOWING_FEED_DATABASE_PATH")
		if databasePath == "" {
			slog.Warn("Scriptable following feed database path not set, using default")
			databasePath = "scriptable_follower_feed_state.db"
		}
		feeds = append(feeds, &ScriptableFollowingFeed{
			FeedActorDID:           os.Getenv("SCRIPTABLE_FOLLOWING_FEED_ACTOR_DID"),
			FeedName:               os.Getenv("SCRIPTABLE_FOLLOWING_FEED_NAME"),
			DatabasePath:           databasePath,
			relayAddress:           relayAddress,
			appviewUrl:             os.Getenv("APPVIEW_URL"),
			runtimes:               make(map[uint64]ScriptRuntime),
			reportChannel:          make(chan int, 1000),
			restartFirehoseChannel: make(chan bool, 1),
		})
	} else {
		slog.Warn("scriptable following feed is disabled, skipping")
	}
	if len(feeds) == 0 {
		return nil, fmt.Errorf("no feeds configured")
	}
	return feeds, nil

}

type FollowingFeed struct {
	FeedActorDID     string
	FeedName         string
	DiscoverFeedName string
	db               *sql.DB
	relayAddress     string
	appviewUrl       string
}

func (ff *FollowingFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	feeds := make([]appbsky.FeedDescribeFeedGenerator_Feed, 0)
	feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
		Uri: "at://" + ff.FeedActorDID + "/app.bsky.feed.generator/" + ff.FeedName,
	})
	if ff.DiscoverFeedName != "" {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + ff.FeedActorDID + "/app.bsky.feed.generator/" + ff.DiscoverFeedName,
		})
	}
	return feeds, nil
}

func (ff *FollowingFeed) GetFeedNames() []string {
	fns := []string{ff.FeedName}
	if ff.DiscoverFeedName != "" {
		fns = append(fns, ff.DiscoverFeedName)
	}
	return fns
}

func (ff *FollowingFeed) getFollowing(userDID string) ([]string, error) {
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

func (ff *FollowingFeed) discoverPage(ctx context.Context, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	_ = ctx
	slog.Info("discover feed page", slog.String("user", userDID), slog.Int64("limit", limit), slog.String("cursor", cursor))
	var cursorAsIndex uint = math.MaxInt64 - 1

	if strings.Contains(cursor, "__") { // idk why social-app does that kind of thing to me
		// ignore their cursor. do nothing
	} else if cursor != "" {
		cursorAsIndexR, err := strconv.ParseUint(cursor, 10, 64)
		if err != nil {
			slog.Error("cursor invalid", slog.String("cursor", cursor), slog.Any("err", err))
			return nil, nil, err
		}
		cursorAsIndex = uint(cursorAsIndexR)
	}

	// hack for now
	query := `
		SELECT at_path, counter
		FROM posts
		WHERE counter < ?
		ORDER BY counter DESC`

	query += fmt.Sprintf(" LIMIT %d", limit)

	fmt.Println(query, cursorAsIndex)
	rows, err := ff.db.Query(query, cursorAsIndex)
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

func (ff *FollowingFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	if ff.DiscoverFeedName != "" && feed == ff.DiscoverFeedName {
		return ff.discoverPage(ctx, userDID, limit, cursor)
	}
	slog.Info("following feed page", slog.String("feed", feed), slog.String("user", userDID), slog.Int64("limit", limit), slog.String("cursor", cursor))

	following, err := ff.getFollowing(userDID)
	if err != nil {
		slog.Error("error getting following", slog.String("user", userDID), slog.Any("err", err))
		return nil, nil, err
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

	// hack for now
	query := `
		SELECT at_path, counter
		FROM posts
		WHERE`

	args := make([]any, 0)
	clauses := make([]string, 0)
	following = append(following, userDID) // let user always see themselves
	for _, followingDID := range following {
		clauses = append(clauses, ` author_did = ? `)
		args = append(args, followingDID)
	}
	query += strings.Join(clauses, "OR")
	if len(clauses) > 0 {
		query += `AND counter < ? `
	} else {
		query += ` counter < ?`
	}
	args = append(args, cursorAsIndex)
	query += `ORDER BY counter DESC`
	query += fmt.Sprintf(" LIMIT %d", limit)

	fmt.Println(query)
	rows, err := ff.db.Query(query, args...)
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

func (ff *FollowingFeed) Spawn(ctx context.Context) {
	f, err := os.OpenFile("following_feed.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		slog.Error("error opening log file", slog.Any("err", err))
		panic("failed to open log file")
	}
	defer f.Close()

	wrt := io.MultiWriter(os.Stderr, f)

	log.SetOutput(wrt)
	if os.Getenv("DEBUG") == "1" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	dbPath := os.Getenv("FOLLOWING_FEED_DATABASE_PATH")
	if dbPath == "" {
		slog.Warn("Following feed database path not set, using default")
		dbPath = "follower_feed_state.db"
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal(err)
	}

	// TODO rest of tables
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

	CREATE TABLE IF NOT EXISTS posts (
		author_did text,
		at_path text,
		counter int unique,
		primary key (author_did, at_path)
	) STRICT;
	CREATE INDEX IF NOT EXISTS posts_author_did_index ON posts (author_did);

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
}

func (ff *FollowingFeed) main(ctx context.Context) {
	defer ff.db.Close()
	for {
		err := ff.firehoseConsumer(ctx)
		if err != nil {
			slog.Error("error in firehose consumer", slog.Any("err", err))
		}
		slog.Info("firehose consumer stopped, restarting in 3 seconds")
		time.Sleep(3 * time.Second)
	}
}

func (ff *FollowingFeed) firehoseConsumer(ctx context.Context) error {
	// TODO store max cursor from firehose for the events we successfully processed and then inject it here
	uri := fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos?cursor=0", ff.relayAddress)
	con, _, err := websocket.DefaultDialer.Dial(uri, http.Header{})
	if err != nil {
		return err
	}
	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
			if err != nil {
				return nil
			}
			userDid := evt.Repo
			for _, op := range evt.Ops {
				slog.Debug("incoming event", slog.String("path", op.Path), slog.Any("cid", op.Cid), slog.String("action", op.Action), slog.String("repo", evt.Repo))
				rcid, recBytes, err := rr.GetRecordBytes(ctx, op.Path)
				if err != nil {
					return nil
				}
				slog.Debug("event", slog.String("rcid", rcid.String()))

				recordType, recordData, err := data.ExtractTypeCBORReader(bytes.NewReader(*recBytes))
				if err != nil {
					return nil
				}
				slog.Debug("record", slog.String("record", recordType))

				switch recordType {
				case "app.bsky.graph.follow":
					rec, err := data.UnmarshalCBOR(recordData)
					if err != nil {
						return nil
					}

					recJSON, err := json.Marshal(rec)
					if err != nil {
						return nil
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
					atPath := fmt.Sprintf("at://%s/%s", userDid, op.Path)
					row := ff.db.QueryRow(`SELECT MAX(counter) FROM posts`)
					var maybeCurrentMaxIndex *uint64
					err := row.Scan(&maybeCurrentMaxIndex)
					if err != nil {
						slog.Error("error getting max index", slog.Any("err", err))
						return nil
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

	sched := sequential.NewScheduler("following_feed", rsc.EventHandler)
	return events.HandleRepoStream(context.Background(), con, sched)
}
