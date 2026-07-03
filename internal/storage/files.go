package storage

import "os"

func removeSQLiteFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		err := os.Remove(candidate)
		if err == nil || os.IsNotExist(err) {
			continue
		}
		return err
	}
	return nil
}
