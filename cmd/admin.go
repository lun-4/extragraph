package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

func wantArg(i int) string {
	arg := os.Args[i]
	if arg == "" {
		log.Fatalln("missing argument", i)
	}
	return arg
}
func wantArgInt(i int) int64 {
	arg := os.Args[i]
	if arg == "" {
		log.Fatalln("missing argument", i)
	}
	v, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		log.Fatalln("expected int on argument ", i, "but got", arg, ":", err.Error())
	}
	return v
}

func main() {
	arg1 := os.Args[1]

	// TODO a better config thing
	databasePath := os.Getenv("SCRIPTABLE_FOLLOWING_FEED_DATABASE_PATH")
	if databasePath == "" {
		databasePath = "scriptable_follower_feed_state.db"
	}

	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	switch arg1 {
	case "ls":
		rows, err := db.Query(`SELECT from_did, state FROM scrape_state`)
		if err != nil {
			panic(err)
		}
		fmt.Println("scrape state:")
		for rows.Next() {
			var did string
			var state string
			rows.Scan(&did, &state)
			fmt.Println(did, state)
		}
	case "adduser":
		did := wantArg(2)
		_, err := db.Exec(`INSERT INTO scrape_state (from_did, state) VALUES (?, ?)`, did, "pending")
		if err != nil {
			panic(err)
		}
	case "setscript":
		did := wantArg(2)
		slot := wantArgInt(3)
		scriptPath := wantArg(4)
		fd, err := os.Open(scriptPath)
		if err != nil {
			log.Fatalln("can't open script", scriptPath, ":", err.Error())
		}
		defer fd.Close()
		scriptBytes, err := io.ReadAll(fd)
		if err != nil {
			log.Fatalln("can't read script", scriptPath, ":", err.Error())
		}
		scriptText := string(scriptBytes)
		_, err = db.Exec(`INSERT INTO scripts (from_did, slot, script) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET script = ?`, did, slot, scriptText, scriptText)
		if err != nil {
			panic(err)
		}
	}
}
