package logger

import (
	"fmt"
	"os"
)

// 检查错误是否为权限问题，如果是，返回true，不是，返回false
func checkPermission(src string) bool {
	_, err := os.Stat(src)
	return os.IsPermission(err)
}

// 检查是否文件或目录是否不存在，如果不存在，返回
func checkNotExist(src string) bool {
	_, err := os.Stat(src)
	return os.IsNotExist(err)
}

func mkDir(src string) error {
	return os.MkdirAll(src, os.ModePerm)
}

func mustOpen(fileName, dir string) (*os.File, error) {
	if checkPermission(dir) {
		return nil, fmt.Errorf("permission denied dir: %s", dir)
	}
	if checkNotExist(dir) {
		if err := mkDir(dir); err != nil {
			return nil, fmt.Errorf("error during make log dir: %s, error: %s", dir, err)
		}
	}
	f, err := os.OpenFile(dir+string(os.PathSeparator)+fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("fail to open file, error: %s", err)
	}
	return f, nil
}
