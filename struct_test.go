/*
* Copyright 2022-2024 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package flynn

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

var userTableName = "testStructUser"
var userDbRef *common.Reference
var userDbPassword = ""

// UserInfo user information context
type UserInfo struct {
	UUID     string
	User     string `flynn:"Name"`
	Picture  string
	EMail    string
	LongName string
	Created  time.Time
}

// User user information
type User struct {
	Info      *UserInfo
	Thumbnail []byte
}

func TestCreateUserInfo(t *testing.T) {
	InitLog(t)

	userURL, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}
	userDbRef, userDbPassword, err = common.NewReference(userURL)
	if !assert.NoError(t, err) {
		log.Log.Fatal("REST audit URL incorrect: " + userURL)
	}
	userDbRef.User = "admin"

	fmt.Printf("Storing audit data to table '%s'\n", userTableName)
	userStoreID, err := Handler(userDbRef, userDbPassword)
	if !assert.NoError(t, err) {
		fmt.Printf("Register error log: %v\n", err)
		return
	}
	defer userStoreID.FreeHandler()
	log.Log.Debugf("Receive user dbid %s", userStoreID)

	err = userStoreID.DeleteTable(userTableName)
	if err != nil {
		fmt.Println("Delete error", err)
	}

	su := &User{}
	err = userStoreID.CreateTable(userTableName, su)
	if !assert.NoError(t, err) {
		fmt.Printf("Database user store creating failed: %v\n", err)
		return
	}
	fmt.Printf("Database user store created successfully\n")
}

func TestInsertUser(t *testing.T) {
	InitLog(t)

	userURL, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}
	userDbRef, userDbPassword, err = common.NewReference(userURL)
	if !assert.NoError(t, err) {
		log.Log.Fatal("REST audit URL incorrect: " + userURL)
	}
	userDbRef.User = "admin"

	fmt.Printf("Storing audit data to table '%s'\n", userTableName)
	userStoreID, err := Handler(userDbRef, userDbPassword)
	if !assert.NoError(t, err) {
		fmt.Printf("Register error log: %v\n", err)
		return
	}
	defer userStoreID.FreeHandler()

	user := "testUser1"
	userInfo := &User{Info: &UserInfo{User: user, Created: time.Now()}}
	insert := &common.Entries{Fields: []string{"*"}, DataStruct: userInfo}
	insert.Values = [][]any{{userInfo}}
	log.Log.Debugf("Insert value %#v", userInfo.Info)
	_, err = userStoreID.Insert(userTableName, insert)
	if !assert.NoError(t, err) {
		return
	}
	user = "testUser2"
	userInfo = &User{Info: &UserInfo{User: user, Created: time.Now()}}
	insert = &common.Entries{Fields: []string{"name", "created"}, DataStruct: userInfo}
	insert.Values = [][]any{{userInfo}}
	log.Log.Debugf("Insert value %#v", userInfo.Info)
	_, err = userStoreID.Insert(userTableName, insert)
	if !assert.NoError(t, err) {
		return
	}

	user = "testUser3"
	userInfo = &User{Info: &UserInfo{User: user}}
	insert = &common.Entries{Fields: []string{"name"}, DataStruct: userInfo}
	insert.Values = [][]any{{userInfo}}
	log.Log.Debugf("Insert value %#v", userInfo.Info)
	_, err = userStoreID.Insert(userTableName, insert)
	if !assert.NoError(t, err) {
		return
	}
	log.Log.Errorf("Error storing user: %v", err)
	err = userStoreID.Commit()
	if !assert.NoError(t, err) {
		return
	}

	found := false
	q := &common.Query{TableName: userTableName,
		Search:     "name IN ('testUser1','testUser2','testUser3')",
		DataStruct: &User{},
		Fields:     []string{"*"}}
	_, err = userStoreID.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, result.Data)
		userInfo = result.Data.(*User)
		fmt.Printf("%#v\n", userInfo.Info)
		fmt.Printf("%s -> %#v\n", userInfo.Info.User, userInfo.Info)
		assert.True(t, userInfo.Info.User == "testUser1" || userInfo.Info.User == "testUser2" ||
			userInfo.Info.User == "testUser3")
		found = true
		return nil
	})
	assert.True(t, found)
	if !assert.NoError(t, err) {
		return
	}

	d, err := userStoreID.Delete(userTableName, &common.Entries{Criteria: q.Search})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, int64(3), d)
}
