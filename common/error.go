package common

import (
	"embed"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Message struct {
	id   uint64
	text string
}

var messageHash = make(map[uint64]*Message)

type Error struct {
	nr   uint64
	args []interface{}
	err  error
}

//go:embed messages
var embedFiles embed.FS

func init() {
	fss, err := embedFiles.ReadDir("messages")
	if err != nil {
		panic("Internal config load error: " + err.Error())
	}
	for _, f := range fss {
		if f.Type().IsRegular() {
			byteValue, err := embedFiles.ReadFile("messages/" + f.Name())
			if err != nil {
				panic("Internal config load error: " + err.Error())
			}
			lines := strings.Split(string(byteValue), "\n")
			for _, line := range lines {
				index := strings.IndexByte(line, '=')
				id, err := strconv.ParseUint(line[:index], 10, 64)
				if err == nil {
					text := line[index+1:]
					messageHash[id] = &Message{id, text}
				} else {
					fmt.Println("Error init msg:", err)
				}
			}
		}
	}
}

func NewError(errNr uint64, args ...interface{}) error {
	var err error
	if len(args) > 0 {
		switch e := args[len(args)-1].(type) {
		case error:
			err = e
		default:
		}
	}
	return &Error{nr: errNr, args: args, err: err}
}

func (e *Error) Error() string {
	outLine := messageHash[e.nr]
	if outLine == nil {
		return fmt.Sprintf("Unknown error ...%05d", e.nr)
	}
	m := outLine.text
	if len(e.args) > 0 {
		m = outLine.convertArgs(e.args...)
	}
	return fmt.Sprintf("DB%03d: %s", e.nr, m)
}

func (m *Message) convertArgs(args ...interface{}) string {
	msg := m.text
	for i, x := range args {
		m := fmt.Sprintf("%v", x)
		c := fmt.Sprintf("\\{%d\\}", i)
		re := regexp.MustCompile(c)
		msg = re.ReplaceAllString(msg, m)
	}
	return msg
}
