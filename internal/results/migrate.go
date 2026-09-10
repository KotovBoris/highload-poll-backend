package results

import (
	"os"
	"path/filepath"
	"sort"
)

// migrationFiles возвращает отсортированный список .sql-файлов в каталоге.
func migrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == ".sql" {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// readFile читает файл целиком.
func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
