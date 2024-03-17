package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"zedis/interface/redis"
	"zedis/logger"
	"zedis/redis/protocol"
)

var originBytes = make([]byte, 0)

// Payload 存储redis.Reply或者error
type Payload struct {
	Data  redis.Reply
	Error error
}

type LineParser func([]byte, *bufio.Reader, chan<- *Payload) error

func parseSingleReply(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	ch <- &Payload{
		Data: protocol.NewSingleReply(string(line[1:])),
	}
	return nil
}

func parseErrorReply(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	ch <- &Payload{
		Data: protocol.NewErrorReply(string(line[1:])),
	}
	return nil
}

func parseIntReply(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	value, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		parseError("illegal number "+string(line[1:]), ch)
		return nil
	}
	ch <- &Payload{
		Data: protocol.NewIntReply(value),
	}
	return nil
}

func parseBulkReply(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		parseError("illegal bulk string header: "+string(header), ch)
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: protocol.NullBulkReplyConst,
		}
	}

	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.NewBulkReply(body[:len(body)-2]),
	}
	return nil
}

func parseMultiBulkReply(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		parseError("illegal array header "+string(header[1:]), ch)
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.EmptyMultiBulkReplyConst,
		}
	}

	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')

		originBytes = append(originBytes, line...)

		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			parseError("illegal bulk string header "+string(line), ch)
			break
		}
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			parseError("illegal bulk string length "+string(line), ch)
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)

			originBytes = append(originBytes, body...)

			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}

	}

	//logger.Infof("origin cmd: %s", bytes.Replace(originBytes, []byte("\r\n"), []byte(" [r][n] "), -1))

	ch <- &Payload{
		Data: protocol.NewMultiBulkReply(lines),
	}
	return nil
}

var parseHandlerMap = map[byte]LineParser{
	'+': parseSingleReply,
	'-': parseErrorReply,
	':': parseIntReply,
	'$': parseBulkReply,
	'*': parseMultiBulkReply,
}

// ParseStream 从Reader读取数据，并通过channel发送payloads
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()

	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')

		originBytes = make([]byte, 0)

		for _, b := range line {
			originBytes = append(originBytes, b)
		}

		if err != nil {
			ch <- &Payload{Error: err}
			close(ch)
			return
		}

		length := len(line)
		if length <= 2 || line[length-1] != '\n' {
			// 可能会在流量复制中出现空行，直接忽略
			continue
		}
		// 去掉CRLF后缀
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		lineHandler, ok := parseHandlerMap[line[0]]
		if !ok {
			lines := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.NewMultiBulkReply(lines),
			}
			continue
		}
		err = lineHandler(line, reader, ch)

		if err != nil {
			ch <- &Payload{Error: err}
			close(ch)
			return
		}

	}
}

func parseError(msg string, ch chan<- *Payload) {
	err := errors.New("parse error: " + msg)
	ch <- &Payload{Error: err}
}
