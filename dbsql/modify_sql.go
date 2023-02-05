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

package dbsql

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

func Insert(dbsql DBsql, name string, insert *common.Entries) error {
	tx, ctx, err := dbsql.StartTransaction()
	if err != nil {
		return err
	}
	if !dbsql.IsTransaction() {
		defer dbsql.Close()
	}

	log.Log.Debugf("Insert SQL table")

	insertCmd := "INSERT INTO " + name + " ("
	values := "("

	indexNeed := dbsql.IndexNeeded()
	for i, field := range insert.Fields {
		if i > 0 {
			insertCmd += ","
			values += ","
		}
		if indexNeed {
			insertCmd += `"` + strings.ToLower(field) + `"`
			values += "$" + strconv.Itoa(i+1)
		} else {
			insertCmd += "`" + strings.ToLower(field) + "`"
			values += "?"
		}
	}
	values += ")"
	insertCmd += ") VALUES " + values
	log.Log.Debugf("Insert pre-CMD: %s", insertCmd)
	for _, v := range insert.Values {
		av := v
		log.Log.Debugf("Insert values: %d -> %#v", len(av), av)
		res, err := tx.ExecContext(ctx, insertCmd, av...)
		if err != nil {
			dbsql.EndTransaction(false)
			log.Log.Debugf("Error insert CMD: %v of %s and cmd %s", err, name, insertCmd)
			return err
		}
		l, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if l == 0 {
			return fmt.Errorf("insert of rows failed")
		}
	}
	if !dbsql.IsTransaction() {
		err = dbsql.EndTransaction(true)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateUpdate(indexNeeded bool, name string, updateInfo *common.Entries) (string, []int) {
	insertCmd := "UPDATE " + name + " SET "

	whereFields := make([]int, 0)
	indexNeed := indexNeeded
	for i, field := range updateInfo.Fields {
		if i > 0 {
			insertCmd += ","
		}
		if indexNeed {
			insertCmd += `"` + strings.ToLower(field) + `"` + "=$" + strconv.Itoa(i+1)
		} else {
			insertCmd += "`" + strings.ToLower(field) + "`" + "=?"
		}
		if slices.Contains(updateInfo.Update, field) {
			whereFields = append(whereFields, i)
		}
	}
	insertCmd += " WHERE "
	return insertCmd, whereFields
}

func generateDelete(indexNeeded bool, name string, valueIndex int, deleteInfo *common.Entries) (string, []any) {
	deleteCmd := "DELETE FROM " + name + " WHERE "

	values := make([]any, 0)
	for i, field := range deleteInfo.Fields {
		if i > 0 {
			deleteCmd += " AND "
		}
		if field[0] == '%' {
			deleteCmd += "(" + field[1:] + " LIKE '" + deleteInfo.Values[0][i].(string) + "')"
			continue
		}
		//deleteCmd += "`" + strings.ToLower(field) + "` IN ("
		deleteCmd += strings.ToLower(field) + " IN ("
		//for j := 0; j < len(deleteInfo.Values[0]); j++ {
		if indexNeeded {
			deleteCmd += "$" + strconv.Itoa(i+1)
		} else {
			deleteCmd += "?"
		}
		values = append(values, deleteInfo.Values[0][i])
		//}
		deleteCmd += ")"
	}
	return deleteCmd, values
}

func Update(dbsql DBsql, name string, updateInfo *common.Entries) (rowsAffected int64, err error) {
	tx, ctx, err := dbsql.StartTransaction()
	if err != nil {
		return -1, err
	}
	if !dbsql.IsTransaction() {
		defer dbsql.Close()
	}
	insertCmd, whereFields := generateUpdate(dbsql.IndexNeeded(), name, updateInfo)
	for i, v := range updateInfo.Values {
		whereClause := createWhere(i, updateInfo, whereFields)
		ic := insertCmd + whereClause
		log.Log.Debugf("Update values: %d -> %#v tx=%v %v", len(v), v, tx, ctx)
		res, err := tx.ExecContext(ctx, ic, v...)
		if err != nil {
			log.Log.Debugf("Update error: %s -> %v", ic, err)
			dbsql.EndTransaction(false)
			return 0, err
		}
		ra, _ := res.RowsAffected()
		rowsAffected += ra
	}
	log.Log.Debugf("Update done")

	err = dbsql.EndTransaction(true)
	if err != nil {
		return -1, err
	}
	return rowsAffected, nil
}

func createWhere(valueIndex int, updateInfo *common.Entries, whereFields []int) string {
	var buffer bytes.Buffer
	for i, x := range updateInfo.Update {
		if strings.ContainsAny(x, "=<>") {
			if i > 0 {
				buffer.WriteString(" AND ")
			}
			buffer.WriteString(x)
		}
	}
	for i, s := range whereFields {
		if buffer.Len() > 0 || i > 0 {
			buffer.WriteString(" AND ")
		}
		buffer.WriteString(`"` + strings.ToLower(updateInfo.Fields[s]) + `"`)
		buffer.WriteRune('=')
		buffer.WriteString(convertString(updateInfo.Values[valueIndex][s]))
	}
	return buffer.String()
}

func convertString(convertToString any) string {
	x := fmt.Sprintf("%v", convertToString)
	switch convertToString.(type) {
	case *string, string:
		return `'` + x + `'`
	}
	return x
}

func Delete(dbsql DBsql, name string, updateInfo *common.Entries) (rowsAffected int64, err error) {
	tx, ctx, err := dbsql.StartTransaction()
	if err != nil {
		return -1, err
	}
	if !dbsql.IsTransaction() {
		defer dbsql.Close()
	}

	for i := 0; i < len(updateInfo.Values); i++ {
		deleteCmd, av := generateDelete(dbsql.IndexNeeded(), name, 0, updateInfo)
		log.Log.Debugf("Delete cmd: %s -> %#v", deleteCmd, av)
		res, err := tx.ExecContext(ctx, deleteCmd, av...)
		if err != nil {
			log.Log.Debugf("Delete error: %v", err)
			dbsql.EndTransaction(false)
			return -1, err
		}
		ra, _ := res.RowsAffected()
		rowsAffected += ra
	}
	err = dbsql.EndTransaction(true)
	if err != nil {
		return -1, err
	}
	log.Log.Debugf("Delete done")
	return
}
