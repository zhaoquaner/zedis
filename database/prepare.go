package database

import "strings"

func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}

func readFirstKey(args [][]byte) ([]string, []string) {
	return nil, []string{string(args[0])}
}

func writeFirstKey(args [][]byte) ([]string, []string) {
	return []string{string(args[0])}, nil
}

func readAllKeys(args [][]byte) ([]string, []string) {
	readKeys := make([]string, 0)
	for _, arg := range args {
		readKeys = append(readKeys, string(arg))
	}
	return nil, readKeys
}

func writeAllKeys(args [][]byte) ([]string, []string) {
	writeKeys := make([]string, 0)
	for _, arg := range args {
		writeKeys = append(writeKeys, string(arg))
	}
	return writeKeys, nil
}

// prepareSetStore Set集合求差、并、交集并存入到新key中的prepare
func prepareSetStore(args [][]byte) ([]string, []string) {
	writeKeys := []string{string(args[0])}
	readKeys := make([]string, 0)
	for i := 1; i < len(args); i++ {
		readKeys = append(readKeys, string(args[i]))
	}
	return writeKeys, readKeys
}

// prepareSInterCard SinterCard命令的prepare
func prepareSInterCard(args [][]byte) ([]string, []string) {
	readKeys := make([]string, 0)
	// 从第二个参数开始，第一个参数是numkeys
	for _, arg := range args[1:] {
		key := string(arg)
		if strings.ToUpper(key) == "LIMIT" {
			break
		}
		readKeys = append(readKeys, key)
	}
	return nil, readKeys
}

// prepareSMove smove命令的prepare
func prepareSMove(args [][]byte) ([]string, []string) {
	writeKeys := []string{string(args[0]), string(args[1])}
	return writeKeys, nil
}

// prepareLmove lmove命令的prepare
func prepareLmove(args [][]byte) ([]string, []string) {
	writeKeys := []string{string(args[0]), string(args[1])}
	return writeKeys, nil
}
