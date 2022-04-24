package raft

import (
	"fmt"
	stdlog "log"
	"os"
	"time"
)

type Logger interface {
	Debug(format string, args ...interface{})
}

func newLogger() *logger {
	return &logger{
		Logger: stdlog.New(
			os.Stdout,
			"",
			0),
	}
}

type logger struct {
	*stdlog.Logger
}

// Debug
func (l *logger) Debug(format string, args ...interface{}) {
	now := time.Now().Format("20060102T15:04:05.000")
	format = fmt.Sprintf("%s %s", now, format)
	l.Logger.Printf(format, args...)
}
