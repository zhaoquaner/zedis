package database

func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}

func ReadFirstKey(args [][]byte) ([]string, []string) {
	return nil, []string{string(args[0])}
}

func WriteFirstKey(args [][]byte) ([]string, []string) {
	return []string{string(args[0])}, nil
}

func ReadAllKeys(args [][]byte) ([]string, []string) {
	readKeys := make([]string, 0)
	for _, arg := range args {
		readKeys = append(readKeys, string(arg))
	}
	return nil, readKeys
}

func WriteAllKeys(args [][]byte) ([]string, []string) {
	writeKeys := make([]string, 0)
	for _, arg := range args {
		writeKeys = append(writeKeys, string(arg))
	}
	return writeKeys, nil
}
