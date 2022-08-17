package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Shimi9999/applysabun"
	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

func main() {
	usageText := "Usage: applysabun songdata.db-path [sabun-dir-path]"
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usageText)
	}
	flag.Parse()

	if len(flag.Args()) == 0 || len(flag.Args()) > 2 {
		fmt.Println(usageText)
		os.Exit(1)
	}

	sddbPath := flag.Arg(0)
	sabunDirPath := "./"
	if len(flag.Args()) == 2 {
		sabunDirPath = flag.Arg(1)
	}

	var err error
	db, err = sqlx.Open("sqlite", sddbPath)
	if err != nil {
		fmt.Println("database open error: %w", err)
		os.Exit(1)
	}
	defer db.Close()
	// TODO:ここで探索対象のテーブルを持つDBファイルか確認するべき

	sabunInfos, err := applysabun.WalkSabunDir(sabunDirPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	searchResults := map[applysabun.MatchingSign][]applysabun.SearchResult{}
	for i := range sabunInfos {
		var result *applysabun.SearchResult
		if sabunInfos[i].LoadingError != nil {
			result = &applysabun.SearchResult{SourceSabunInfo: &sabunInfos[i], Sign: applysabun.ERROR}
		} else {
			result, err = applysabun.SearchBmsDirPathFromSDDB(&sabunInfos[i], db)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
		fmt.Println(result.String())
		searchResults[result.Sign] = append(searchResults[result.Sign], *result)
	}

	if len(searchResults) == 0 {
		fmt.Println("BMS file not found.")
		os.Exit(1)
	}
	fmt.Printf("\nOK:%d, NG:%d, EXIST:%d, ERROR:%d\n",
		len(searchResults[applysabun.OK]), len(searchResults[applysabun.NG]), len(searchResults[applysabun.EXIST]), len(searchResults[applysabun.ERROR]))
	if len(searchResults[applysabun.OK]) == 0 {
		fmt.Println("No OK sabun.")
		os.Exit(1)
	}

	fmt.Printf("Move %d OK sabuns?\n", len(searchResults[applysabun.OK]))
	var answer string
	for answer != "y" && answer != "n" {
		fmt.Printf("(y/n): ")
		fmt.Scan(&answer)
		answer = strings.ToLower(answer)
	}
	if answer == "n" {
		fmt.Println("Canceled")
		os.Exit(0)
	}
	fmt.Println("")

	if err := applysabun.MoveOkSabunFilesAndAdditionalSoundFiles(sabunDirPath, searchResults[applysabun.OK]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("\nDone")
	os.Exit(0)
}
