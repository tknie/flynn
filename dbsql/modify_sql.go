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
	log.Log.Debugf("Transaction (begin insert): %v", dbsql.IsTransaction())
	tx, ctx, err := dbsql.StartTransaction()
	if err != nil {
		return err
	}
	log.Log.Debugf("Transaction %p", tx)
	if !dbsql.IsTransaction() {
		log.Log.Debugf("Init defer close ... in inserting")
		defer dbsql.Close()
	}

	log.Log.Debugf("Insert SQL record")

	insertCmd := "INSERT INTO " + name + " ("
	values := "("

	indexNeed := dbsql.IndexNeeded()
	var insertValues [][]any
	var insertFields []string
	if insert.DataStruct != nil {
		dynamic := common.CreateInterface(insert.DataStruct, insert.Fields)
		insertFields = dynamic.RowFields
		v := dynamic.CreateInsertValues()
		insertValues = [][]any{v}
		log.Log.Debugf("Row   fields: %#v", insertFields)
		log.Log.Debugf("Value fields: %#v", insertValues)
	} else {
		insertFields = insert.Fields
		insertValues = insert.Values
	}
	for i, field := range insertFields {
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
	for _, v := range insertValues {
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
	log.Log.Debugf("Transaction: %v", dbsql.IsTransaction())
	if !dbsql.IsTransaction() {
		log.Log.Debugf("No transaction, end and close")
		err = dbsql.EndTransaction(true)
		if err != nil {
			log.Log.Debugf("Error transaction %v", err)
			dbsql.Close()
			return err
		}
		log.Log.Debugf("Close ...")
		dbsql.Close()
	} else {
		log.Log.Debugf("Transaction, NO end and close")
	}
	return nil
}

func GenerateUpdate(indexNeeded bool, name string, updateInfo *common.Entries) (string, []int) {
	insertCmd := "UPDATE " + name + " SET "

	whereFields := make([]int, 0)
	indexNeed := indexNeeded
	var insertFields []string
	if updateInfo.DataStruct != nil {
		dynamic := common.CreateInterface(updateInfo.DataStruct, updateInfo.Fields)
		insertFields = dynamic.RowFields
	} else {
		insertFields = updateInfo.Fields
	}

	for i, field := range insertFields {
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

func GenerateDelete(indexNeeded bool, name string, valueIndex int, deleteInfo *common.Entries) (string, []any) {
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
	log.Log.Debugf("Transaction %p", tx)
	if !dbsql.IsTransaction() {
		log.Log.Debugf("Is no transaction closing after update")
		defer dbsql.Close()
	}
	insertCmd, whereFields := GenerateUpdate(dbsql.IndexNeeded(), name, updateInfo)
	log.Log.Debugf("CMD: %s - %s", insertCmd, whereFields)
	var insertValues [][]any
	if updateInfo.DataStruct != nil {
		dynamic := common.CreateInterface(updateInfo.DataStruct, updateInfo.Fields)
		v := dynamic.CreateInsertValues()
		insertValues = [][]any{v}
		log.Log.Debugf("Value fields: %#v", insertValues)
	} else {
		insertValues = updateInfo.Values
	}
	for i, v := range insertValues {
		whereClause := CreateWhere(i, updateInfo, whereFields)
		ic := insertCmd + whereClause
		log.Log.Debugf("Update CMD: %s", ic)
		log.Log.Debugf("Update values: %d -> %#v", len(v), v)
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

	log.Log.Debugf("Transaction: %v", dbsql.IsTransaction())
	if !dbsql.IsTransaction() {
		log.Log.Debugf("No transaction, end and close")
		err = dbsql.EndTransaction(true)
		if err != nil {
			log.Log.Debugf("Error transaction %v", err)
			dbsql.Close()
			return 0, err
		}
		log.Log.Debugf("Close ...")
		dbsql.Close()
	} else {
		log.Log.Debugf("Transaction, NO end and close")
	}

	return rowsAffected, nil
}

func CreateWhere(valueIndex int, updateInfo *common.Entries, whereFields []int) string {
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

	if updateInfo.Criteria != "" {
		deleteCmd := "DELETE FROM " + name + " WHERE " + updateInfo.Criteria

		log.Log.Debugf("Delete cmd: %s", deleteCmd)
		res, err := tx.ExecContext(ctx, deleteCmd)
		if err != nil {
			log.Log.Debugf("Delete error: %v", err)
			dbsql.EndTransaction(false)
			return -1, err
		}

		ra, _ := res.RowsAffected()
		rowsAffected += ra
	} else {
		for i := 0; i < len(updateInfo.Values); i++ {
			deleteCmd, av := GenerateDelete(dbsql.IndexNeeded(), name, 0, updateInfo)
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
	}
	err = dbsql.EndTransaction(true)
	if err != nil {
		return -1, err
	}
	log.Log.Debugf("Delete done")
	return
}
