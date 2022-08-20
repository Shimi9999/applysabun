package applysabun

import (
	"fmt"
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

func OpenSongdb(path string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type SabunInfo struct {
	BmsData                  *gobms.BmsData
	AdditionalSoundFilePaths []string
	LoadingError             error
	TargetSearchResult       *SearchResult
}

func WalkSabunDir(sabunDirPath string) (sabunInfos []SabunInfo, _ error) {
	files, err := os.ReadDir(sabunDirPath)
	if err != nil {
		return nil, fmt.Errorf("ReadDir: %w", err)
	}

	// 直下の差分、直下の音源ファイル
	underDirSabunPaths := []string{}
	underDirSoundFilePaths := []string{}

	for _, file := range files {
		path := filepath.Join(sabunDirPath, file.Name())
		if file.IsDir() {
			sis, err := WalkSabunDir(path)
			if err != nil {
				return nil, fmt.Errorf("walkSabunDir %s: %w", path, err)
			}
			sabunInfos = append(sabunInfos, sis...)
		} else if gobms.IsBmsPath(path) {
			underDirSabunPaths = append(underDirSabunPaths, path)
		} else if isBmsSoundPath(path) {
			underDirSoundFilePaths = append(underDirSoundFilePaths, path)
		}
	}

	// bmsデータのロードと追加音源ファイル一覧の作成
	for _, udSabunPath := range underDirSabunPaths {
		bmsData, err := loadBms(udSabunPath)
		if err != nil {
			if strings.HasPrefix(err.Error(), "Timeout LoadBms: ") {
				fmt.Println(err)
				// ローディングがタイムアウトしたらダミーデータを追加する
				sabunInfos = append(sabunInfos, SabunInfo{
					BmsData:      &gobms.BmsData{Path: udSabunPath},
					LoadingError: err})
				continue
				// skip
			} else {
				return nil, fmt.Errorf("loadBms %s: %w", udSabunPath, err)
			}
		}

		additionalSoundFilePaths := []string{}
		wavDefs := copyMap(bmsData.UniqueBmsData.WavDefs)
		for _, path := range underDirSoundFilePaths {
			for key, wavDef := range wavDefs {
				if getPureFileName(path) == getPureFileName(wavDef) {
					//fmt.Printf("match!: %s %s\n", path, wavDef)
					additionalSoundFilePaths = append(additionalSoundFilePaths, path)
					delete(wavDefs, key)
				}
			}
		}

		sabunInfos = append(sabunInfos, SabunInfo{
			BmsData:                  bmsData,
			AdditionalSoundFilePaths: additionalSoundFilePaths,
			LoadingError:             nil})
	}

	return sabunInfos, nil
}

func getPureFileName(path string) string {
	return filepath.Base(path[:len(path)-len(filepath.Ext(path))])
}

func copyMap(srcMap map[string]string) map[string]string {
	dstMap := map[string]string{}
	for key, value := range srcMap {
		dstMap[key] = value
	}
	return dstMap
}

func loadBms(path string) (bmsData *gobms.BmsData, err error) {
	// 非常に長いBMSの読み込みはTimeOutで失敗させてスキップする
	doneLoadBms := make(chan interface{})
	go func() {
		var _bmsData gobms.BmsData
		_bmsData, err = gobms.LoadBms(path)
		bmsData = &_bmsData
		close(doneLoadBms)
	}()
	select {
	case <-doneLoadBms:
		if err != nil {
			return nil, fmt.Errorf("Failed LoadBms: %w", err)
		}
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("Timeout LoadBms: %s", path)
	}
	return bmsData, nil
}

func isBmsSoundPath(path string) bool {
	ext := filepath.Ext(path)
	soundExts := []string{".wav", ".ogg", ".flac", ".mp3"}
	for _, soundExt := range soundExts {
		if strings.ToLower(ext) == soundExt {
			return true
		}
	}
	return false
}

type Chart struct {
	Title  string `db:"title"`
	Genre  string `db:"genre"`
	Artist string `db:"artist"`
	Path   string `db:"path"`
}

type MatchingResult int

const (
	Unmatch MatchingResult = iota
	Maybe
	GenreConditional
	ArtistConditional
	Almost
	Perfect
)

func (m MatchingResult) String() string {
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

type MatchingSign string

const (
	OK    MatchingSign = "OK"
	NG    MatchingSign = "NG"
	EXIST MatchingSign = "EXIST"
	ERROR MatchingSign = "ERROR"
)

type SearchResult struct {
	Sign             MatchingSign
	TargetBmsDirPath string
	MatchingLevel    MatchingResult
}

func (r SearchResult) String(sourceSabunInfo *SabunInfo) string {
	str := fmt.Sprintf("%s: %s", r.Sign, sourceSabunInfo.BmsData.Path)
	if r.Sign == ERROR {
		if sourceSabunInfo.LoadingError != nil && strings.HasPrefix(sourceSabunInfo.LoadingError.Error(), "Timeout LoadBms: ") {
			str += " -- loading timeout"
		} else {
			str += " -- something error"
		}
	} else {
		if r.Sign != NG {
			str += fmt.Sprintf(" -> %s", r.TargetBmsDirPath)
		}
		if r.Sign != EXIST {
			str += fmt.Sprintf(" (Matching: %s)", r.MatchingLevel)
		}
	}
	return str
}

func SearchBmsDirPathFromSDDB(bmsData *gobms.BmsData, db *sqlx.DB) (result *SearchResult, _ error) {
	result = &SearchResult{}

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
		result.TargetBmsDirPath = filepath.Dir(c.Path)
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
	var bestMatchingResult MatchingResult
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

		var matchingResult MatchingResult
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
		result.TargetBmsDirPath = filepath.Dir(bestChart.Path)
	}
	//fmt.Printf("  %s -> %s\n", bestMatchingResult.String(), bmsDirPath)
	//fmt.Print(bestLog)

	//log = fmt.Sprintf("%s: %s -> %s (Matching: %s)", okngStr, bmsData.Path, bmsDirPath, bestMatchingResult)

	return result, nil
}

func MoveSabunFileAndAdditionalSoundFiles(sabunDirPath string, sabunInfo *SabunInfo) error {
	getTargetPath := func(dir, src string, duplicationNum int) string {
		base := filepath.Base(src)
		if duplicationNum == 0 {
			return filepath.Join(dir, base)
		} else {
			ext := filepath.Ext(base)
			name := base[:len(base)-len(ext)]
			return filepath.Join(dir, fmt.Sprintf("%s (%d)%s", name, duplicationNum, ext))
		}
	}

	move := func(sourcePath, targetDirPath string, isSabun bool) error {
		var targetPath string
		if isSabun {
			// ファイル名が重複したらナンバリングを追加して再試行
			for i := 0; ; i++ {
				targetPath = getTargetPath(targetDirPath, sourcePath, i)
				if fileExists(targetPath) {
					// ファイル名が同じで内容も同じファイルが存在するなら、ファイル移動処理をスキップする
					if same, err := isSameFile(sourcePath, targetPath); err != nil {
						return fmt.Errorf("Failed isSameFile: %w", err)
					} else if same {
						fmt.Printf("Skip because the same file already exist: %s %s\n", sourcePath, targetPath)
						return nil
					}
				} else {
					break
				}
			}
		} else {
			targetPath = getTargetPath(targetDirPath, sourcePath, 0)
			// bmsファイル以外(追加音源ファイルなど)は、移動先に同名ファイルが存在したら、移動処理をスキップする
			if fileExists(targetPath) {
				fmt.Printf("Skip because the same file already exist: %s %s\n", sourcePath, targetPath)
				return nil
			}
		}

		//fmt.Printf("move %s => %s\n", sourcePath, targetPath)

		if err := moveFile(sourcePath, targetPath); err != nil {
			return fmt.Errorf("Failed to move: %w", err)
		}
		fmt.Printf("Moved: %s -> %s\n", sourcePath, targetPath)

		// move後のディレクトリが空(もしくは.txtファイルのみ)ならディレクトリを削除する
		movedDirPath := filepath.Dir(sourcePath)
		if filepath.Clean(movedDirPath) != filepath.Clean(sabunDirPath) {
			if removed, err := removeEmptyDirectory(movedDirPath); err != nil {
				return fmt.Errorf("Failed to remove empty directory: %w", err)
			} else if removed {
				fmt.Printf("- Removed empty dir: %s\n", movedDirPath)
			}
		}

		return nil
	}

	if sabunInfo.TargetSearchResult == nil {
		return fmt.Errorf("TargetSearchResult is nil")
	}

	// 差分BMSファイル移動
	if err := move(sabunInfo.BmsData.Path, sabunInfo.TargetSearchResult.TargetBmsDirPath, true); err != nil {
		return err
	}
	// 追加音源ファイル移動
	// TODO 音源が直下でなくディレクトリ内にある場合、移動先にディレクトリを作る必要があるかも？
	for _, AdditionalSoundFilePath := range sabunInfo.AdditionalSoundFilePaths {
		if err := move(AdditionalSoundFilePath, sabunInfo.TargetSearchResult.TargetBmsDirPath, false); err != nil {
			return err
		}
	}

	return nil
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// パーティションをまたぐことが可能なファイル移動
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
