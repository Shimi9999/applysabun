package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/Shimi9999/gobms"
	"github.com/hbollon/go-edlib"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
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

	sabunPaths, err := walkSabunDir(sabunDirPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	searchResults := map[matchingSign][]SearchResult{}
	for _, sabunPath := range sabunPaths {
		result, err := searchBmsDir(sabunPath)
		if err != nil {
			fmt.Println(err)
			if strings.HasPrefix(err.Error(), "Timeout LoadBms: ") {
				// skip
			} else {
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
	if len(searchResults[OK]) == 0 {
		fmt.Println("No OK sabuns.")
		os.Exit(1)
	}
	fmt.Printf("\nOK:%d, NG:%d, EXIST:%d\n", len(searchResults[OK]), len(searchResults[NG]), len(searchResults[EXIST]))

	fmt.Printf("Move %d OK sabuns?\n", len(searchResults[OK]))
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

	getTargetPath := func(dir, src string, i int) string {
		base := filepath.Base(src)
		if i == 0 {
			return filepath.Join(dir, base)
		} else {
			ext := filepath.Ext(base)
			name := base[:len(base)-len(ext)]
			return filepath.Join(dir, fmt.Sprintf("%s (%d)%s", name, i, ext))
		}
	}
	for _, r := range searchResults[OK] {
		sourceBmsPath := r.SourceBmsData.Path
		var targetBmsPath string
		isSkip := false
		for i := 0; ; i++ {
			targetBmsPath = getTargetPath(r.ResultBmsDirPath, r.SourceBmsData.Path, i)
			if _, err := os.Stat(targetBmsPath); err != nil {
				break
			} else {
				// ファイル名が同じで内容も同じファイルが存在するなら、ファイル移動処理をスキップする
				if same, err := isSameFile(sourceBmsPath, targetBmsPath); err != nil {
					fmt.Println("Failed isSameFile: %w", err)
					os.Exit(1)
				} else if same {
					isSkip = true
					fmt.Printf("Skip because the same file already exist: %s %s\n", sourceBmsPath, targetBmsPath)
					break
				}
			}
		}
		if isSkip {
			continue
		}

		if err := moveFile(sourceBmsPath, targetBmsPath); err != nil {
			fmt.Println("Failed to move sabun: %w", err)
			os.Exit(1)
		}
		fmt.Printf("Moved: %s -> %s\n", sourceBmsPath, targetBmsPath)

		// move後のディレクトリが空(もしくは.txtファイルのみ)ならディレクトリを削除する
		movedDirPath := filepath.Dir(sourceBmsPath)
		if filepath.Clean(movedDirPath) != filepath.Clean(sabunDirPath) {
			if removed, err := removeEmptyDirectory(movedDirPath); err != nil {
				fmt.Println("Failed to remove empty directory: %w", err)
				os.Exit(1)
			} else if removed {
				fmt.Printf("- Removed empty dir: %s\n", movedDirPath)
			}
		}
	}
	fmt.Println("\nDone")
	os.Exit(0)
}

// パーティションをまたぐファイル移動を可能にする
func moveFile(sourcePath, targetPath string) error {
	sourceBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	if err := os.WriteFile(targetPath, sourceBytes, 0664); err != nil {
		return err
	}

	if err := os.Remove(sourcePath); err != nil {
		return err
	}

	return nil
}

func removeEmptyDirectory(dirPath string) (bool, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return false, err
	}

	isEmpty := true
	for _, file := range files {
		if strings.ToLower(filepath.Ext(file.Name())) == ".txt" {
			// 何もしない
		} else {
			isEmpty = false
			break
		}
	}
	if isEmpty {
		err := os.RemoveAll(dirPath)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func isSameFile(path1, path2 string) (bool, error) {
	bytes1, err := os.ReadFile(path1)
	if err != nil {
		return false, err
	}
	bytes2, err := os.ReadFile(path2)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(bytes1, bytes2), nil
}

func walkSabunDir(sabunDirPath string) (sabunPaths []string, _ error) {
	err := filepath.WalkDir(sabunDirPath, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("Failed WalkDir: %w", err)
		}

		if gobms.IsBmsPath(path) {
			sabunPaths = append(sabunPaths, path)
			//fmt.Println("isSabun:", path)
		}

		return nil
	})
	return sabunPaths, err
}

func searchBmsDir(sabunPath string) (*SearchResult, error) {
	// 非常に長いBMSの読み込みはTimeOutで失敗させてスキップする
	doneLoadBms := make(chan interface{})
	var bmsData gobms.BmsData
	var err error
	go func() {
		bmsData, err = gobms.LoadBms(sabunPath)
		close(doneLoadBms)
	}()
	select {
	case <-doneLoadBms:
		if err != nil {
			return nil, fmt.Errorf("Failed LoadBms: %w", err)
		}
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("Timeout LoadBms: %s", sabunPath)
	}

	result, err := bmsDirPathFromSDDB(&bmsData)
	if err != nil {
		return nil, fmt.Errorf("Failed bmsDirPathFromSDDB: %w", err)
	}

	return result, nil
}

type Chart struct {
	Title  string `db:"title"`
	Genre  string `db:"genre"`
	Artist string `db:"artist"`
	Path   string `db:"path"`
}

type matchingResult int

const (
	Unmatch matchingResult = iota
	Maybe
	GenreConditional
	ArtistConditional
	Almost
	Perfect
)

func (m matchingResult) String() string {
	switch m {
	case Perfect:
		return "★ Perfect"
	case Almost:
		return "☆ Almost"
	case ArtistConditional:
		return "〇Artist Conditional"
	case GenreConditional:
		return "◇ Genre Conditional"
	case Maybe:
		return "△ Maybe"
	default:
		return "✕ Unmatch"
	}
}

type matchingSign string

const (
	OK    = "OK"
	NG    = "NG"
	EXIST = "EXIST"
)

type SearchResult struct {
	Sign             matchingSign
	SourceBmsData    *gobms.BmsData
	ResultBmsDirPath string
	MatchingLevel    matchingResult
}

func (r SearchResult) String() string {
	str := fmt.Sprintf("%s: %s", r.Sign, r.SourceBmsData.Path)
	if r.Sign != NG {
		str += fmt.Sprintf(" -> %s", r.ResultBmsDirPath)
	}
	if r.Sign != EXIST {
		str += fmt.Sprintf(" (Matching: %s)", r.MatchingLevel)
	}
	return str
}

func bmsDirPathFromSDDB(bmsData *gobms.BmsData) (result *SearchResult, _ error) {
	result = &SearchResult{}
	result.SourceBmsData = bmsData

	// 既に同じsha256の譜面が存在するかを確認
	rows, err := db.Queryx("SELECT path FROM song WHERE sha256 = $1", bmsData.Sha256)
	if err != nil {
		return nil, fmt.Errorf("Failed db.Query: %w", err)
	}
	if rows.Next() {
		var c Chart
		err := rows.StructScan(&c)
		if err != nil {
			return nil, fmt.Errorf("Failed rows.StructScan: %w", err)
		}
		result.Sign = EXIST
		result.ResultBmsDirPath = filepath.Dir(c.Path)
		return result, nil
	}

	pureTitle := gobms.RemoveSuffixChartName(bmsData.Title)
	rows, err = db.Queryx("SELECT title, genre, artist, path FROM song WHERE title LIKE $1", pureTitle+"%")
	if err != nil {
		return nil, fmt.Errorf("Failed db.Query: %w", err)
	}
	defer rows.Close()

	//fmt.Printf("%s, %s, %s: %s\n", bmsData.Title, bmsData.Artist, bmsData.Genre, bmsData.Path)
	var bestChart Chart
	var bestMatchingResult matchingResult
	var bestLog string
	for rows.Next() {
		var c Chart
		err := rows.StructScan(&c)
		if err != nil {
			return nil, fmt.Errorf("Failed rows.StructScan: %w", err)
		}

		//fmt.Printf("  title: %s, path: %s\n", c.Title, c.Path)

		stringsSimilarityError := func(err error) error {
			return fmt.Errorf("Failed StringsSimilarity: %w", err)
		}

		cPureTitle := gobms.RemoveSuffixChartName(c.Title)
		ts, err := edlib.StringsSimilarity(pureTitle, cPureTitle, edlib.Levenshtein)
		if err != nil {
			return nil, stringsSimilarityError(err)
		}

		as, err := edlib.StringsSimilarity(bmsData.Artist, c.Artist, edlib.Levenshtein)
		if err != nil {
			return nil, stringsSimilarityError(err)
		}

		pureGenre := gobms.RemoveSuffixChartName(bmsData.Genre)
		cPureGenre := gobms.RemoveSuffixChartName(c.Genre)
		gs, err := edlib.StringsSimilarity(pureGenre, cPureGenre, edlib.Levenshtein)
		if err != nil {
			return nil, stringsSimilarityError(err)
		}

		var matchingResult matchingResult
		if ts == 1.0 && as == 1.0 && gs == 1.0 {
			matchingResult = Perfect
		} else if ts >= 0.9 && as >= 0.9 && gs >= 0.9 {
			matchingResult = Almost
		} else if ts >= 0.9 && strings.HasPrefix(bmsData.Artist, c.Artist) && gs >= 0.9 {
			matchingResult = ArtistConditional
		} else if ts >= 0.9 && as >= 0.9 {
			matchingResult = GenreConditional
		} else if ts >= 0.8 && as+gs >= 1.5 {
			matchingResult = Maybe
		} else {
			matchingResult = Unmatch
		}

		if matchingResult > bestMatchingResult {
			bestMatchingResult = matchingResult
			bestChart = c
			bestLog = fmt.Sprintf("    %s: %f, %f, %f: (%s - %s)(%s - %s)(%s - %s)\n", matchingResult.String(), ts, as, gs, pureTitle, cPureTitle, bmsData.Artist, c.Artist, pureGenre, cPureGenre)
		}
		if matchingResult == Perfect {
			break
		}

		if bestMatchingResult == Unmatch && ts+as+gs >= 1.5 {
			bestLog = fmt.Sprintf("    %s: %f, %f, %f: (%s - %s)(%s - %s)(%s - %s)\n", matchingResult.String(), ts, as, gs, pureTitle, cPureTitle, bmsData.Artist, c.Artist, pureGenre, cPureGenre)
		}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("rows scan error: %w", err)
	}

	result.MatchingLevel = bestMatchingResult
	if bestMatchingResult == Unmatch {
		result.Sign = NG
		//fmt.Printf("  %s\n", bestMatchingResult.String())
		if bestLog != "" {
			//fmt.Print(bestLog)
		}
	} else {
		result.Sign = OK
		result.ResultBmsDirPath = filepath.Dir(bestChart.Path)
	}
	//fmt.Printf("  %s -> %s\n", bestMatchingResult.String(), bmsDirPath)
	//fmt.Print(bestLog)

	//log = fmt.Sprintf("%s: %s -> %s (Matching: %s)", okngStr, bmsData.Path, bmsDirPath, bestMatchingResult)

	return result, nil
}
