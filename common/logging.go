package common

import (
	"fmt"
	"strings"
	"time"
)

type nilLogger struct {
}

func lognil() *nilLogger {
	return &nilLogger{}
}

func (*nilLogger) Debugf(format string, args ...interface{}) {
}

func (*nilLogger) Infof(format string, args ...interface{}) {
}

func (*nilLogger) Errorf(format string, args ...interface{}) {
}

func (*nilLogger) Fatal(args ...interface{}) {
}

// Log defines the log interface to manage other Log output frameworks
type LogI interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
}

// Central central configuration
var Log = LogI(lognil())
var debug = false

func IsDebugLevel() bool {
	return debug
}

func (log *nilLogger) SetDebugLevel(debugIn bool) {
	debug = debugIn
	if debug {
		fmt.Println("Warning debug is enabled")
	}
}

// LogMultiLineString log multi line string to log. This prevent the \n display in log.
// Instead multiple lines are written to log
func LogMultiLineString(debug bool, logOutput string) {
	if debug && !IsDebugLevel() {
		return
	}
	columns := strings.Split(logOutput, "\n")
	for _, c := range columns {
		if debug {
			Log.Debugf("%s", c)
		} else {
			Log.Errorf("%s", c)
		}
	}
}

// TimeTrack defer function measure the difference end log it to log management, like
//
//	defer TimeTrack(time.Now(), "Info")
func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	Log.Infof("%s took %s", name, elapsed)
}
