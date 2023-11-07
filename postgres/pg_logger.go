/*
* Copyright 2022-2023 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/tracelog"
	"github.com/tknie/log"
)

type Logger struct {
}

func NewLogger() *Logger {
	return &Logger{}
}

func (pl *Logger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
	fields := make([]interface{}, len(data))
	i := 0
	for k, v := range data {
		log.Log.Debugf("%v=%v", k, v)
		fields[i] = v
		i++
	}

	switch level {
	case tracelog.LogLevelTrace:
		log.Log.Debugf("PGX_LOG_LEVEL "+msg, fields...)
	case tracelog.LogLevelDebug:
		log.Log.Debugf("PGX_LOG_LEVEL "+msg, fields...)
	case tracelog.LogLevelInfo:
		log.Log.Infof("PGX_LOG_LEVEL "+msg, fields...)
	case tracelog.LogLevelWarn:
		log.Log.Infof("PGX_LOG_LEVEL "+msg, fields...)
	case tracelog.LogLevelError:
		log.Log.Errorf("PGX_LOG_LEVEL "+msg, fields...)
	default:
		log.Log.Errorf("PGX_LOG_LEVEL "+msg, fields...)
	}
}
