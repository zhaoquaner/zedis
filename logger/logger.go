package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type Settings struct {
	Path       string `yaml:"Path"`
	Name       string `yaml:"Name"`
	Ext        string `yaml:"Ext"`
	TimeFormat string `yaml:"TimeFormat"`
}

type logLevel int

const (
	DEBUG logLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const (
	flags              = log.LstdFlags | log.Lmicroseconds
	defaultCallerDepth = 2
	bufferSize         = 1e5 // 日志池的大小，表示日志消息池最大能保存多少消息
)

type logEntry struct {
	msg   string
	level logLevel
}

var (
	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

// Logger 定义了一个日志结构体，字段包括日志文件；log库对象；一个日志channel，用于接受日志消息；日志消息池
type Logger struct {
	logFile   *os.File
	logger    *log.Logger
	entryChan chan *logEntry
	entryPool *sync.Pool
}

// DefaultLogger 默认日志对象
var DefaultLogger = NewStdoutLogger()

// NewStdoutLogger 新建一个向标准控制台输出的logger
func NewStdoutLogger() *Logger {
	logger := &Logger{
		logFile:   nil,
		logger:    log.New(os.Stdout, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() any {
				return &logEntry{}
			},
		},
	}
	go func() {
		for e := range logger.entryChan {
			_ = logger.logger.Output(0, e.msg)
			logger.entryPool.Put(e)
		}
	}()
	return logger
}

func NewFileLogger(settings *Settings) (*Logger, error) {
	fileName := fmt.Sprintf("%s-%s.%s", settings.Name, time.Now().Format(settings.TimeFormat), settings.Ext)
	logFile, err := mustOpen(fileName, settings.Path)
	if err != nil {
		return nil, fmt.Errorf("open log file error:%v", err)
	}
	// 多路写对象，可以同时向标准输出和日志文件写日志
	mw := io.MultiWriter(os.Stdout, logFile)
	logger := &Logger{
		logFile:   logFile,
		logger:    log.New(mw, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			// 如果日志池为空，则会调用该方法创建一个新对象
			New: func() any {
				return &logEntry{}
			},
		},
	}

	go func() {
		for e := range logger.entryChan {
			// 每次收到日志消息，首先判断要写到哪个日志文件，根据时间、文件后缀等字段拼出日志文件名，如果和logger当前logFile的Name不一致，说明需要写入到一个新日志文件
			logFileName := fmt.Sprintf("%s-%s.%s", settings.Name, time.Now().Format(settings.TimeFormat), settings.Ext)
			if path.Join(settings.Path, logFileName) != logger.logFile.Name() {
				logFile, err := mustOpen(logFileName, settings.Path)
				if err != nil {
					panic("open log file " + logFileName + " failed: %s" + err.Error())
				}
				// 关闭原有文件对象
				_ = logger.logFile.Close()
				// 赋值新的文件对象
				logger.logFile = logFile
				logger.logger = log.New(io.MultiWriter(os.Stdout, logFile), "", flags)
			}
			_ = logger.logger.Output(0, e.msg)
			logger.entryPool.Put(e)
		}
	}()

	return logger, nil
}

func Setup(setting *Settings) {
	logger, err := NewFileLogger(setting)
	if err != nil {
		panic(err)
	}
	DefaultLogger = logger
}

// Output 发送一个日志消息到logger
func (logger *Logger) Output(level logLevel, callerDepth int, msg string) {
	var formattedMsg string
	// file表示调用该函数的文件，line表示对应行号，ok表示是否获取成功
	_, file, line, ok := runtime.Caller(2)
	if ok {
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}

	entry := logger.entryPool.Get().(*logEntry)
	entry.msg = formattedMsg
	entry.level = level
	logger.entryChan <- entry
}

func Debug(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

func Debugf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

func Info(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

func Infof(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

func Warn(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(WARNING, defaultCallerDepth, msg)
}

func Warnf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(WARNING, defaultCallerDepth, msg)
}

func Error(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

func Errorf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

func Fatal(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(FATAL, defaultCallerDepth, msg)
}

func Fatalf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(FATAL, defaultCallerDepth, msg)
}
