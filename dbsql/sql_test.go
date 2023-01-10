package dbsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/db/common"
)

func TestSQLUpdate(t *testing.T) {
	ui := &common.Entries{
		Fields: []string{"ABC", "BCD", "YYY"},
		Update: []string{"ABC"},
		Values: [][]any{{"abc", 123, 233}},
	}
	sqlCmd, rows := generateUpdate(true, "ABC", ui)
	assert.Equal(t, "UPDATE ABC SET ABC=$1,BCD=$2,YYY=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{0}, rows)
	wh := createWhere(0, ui, rows)
	assert.Equal(t, "ABC=abc", wh)

	ui.Update[0] = "BCD"
	sqlCmd, rows = generateUpdate(true, "DFX", ui)
	assert.Equal(t, "UPDATE DFX SET ABC=$1,BCD=$2,YYY=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{1}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "BCD=123", wh)

	ui.Update[0] = "BCXD=hugo"
	sqlCmd, rows = generateUpdate(true, "XYY", ui)
	assert.Equal(t, "UPDATE XYY SET ABC=$1,BCD=$2,YYY=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "BCXD=hugo", wh)

	ui.Update[0] = "DDD=emil"
	ui.Update = append(ui.Update, "YYY")
	sqlCmd, rows = generateUpdate(true, "XYY", ui)
	assert.Equal(t, "UPDATE XYY SET ABC=$1,BCD=$2,YYY=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{2}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "DDD=emil AND YYY=233", wh)

	ui.Update[0] = "YYY=emil"
	ui.Update = append(ui.Update, "ABC")
	ui.Update = append(ui.Update, "WWW=abc")
	sqlCmd, rows = generateUpdate(true, "XYY", ui)
	assert.Equal(t, "UPDATE XYY SET ABC=$1,BCD=$2,YYY=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{0, 2}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "YYY=emil AND WWW=abc AND ABC=abc AND YYY=233", wh)

	ui.Fields = []string{"AA", "BB", "CC", "DD", "TT"}
	ui.Values = [][]any{{"XXX", "daslkds", 123, 222, 222, time.Now()}, {"XXX2", "aaa2", 51, 522, 5222, time.Now()}}
	ui.Update = []string{"YY=otto", "AA", "CC", "TT"}
	sqlCmd, rows = generateUpdate(true, "XYY", ui)
	assert.Equal(t, "UPDATE XYY SET AA=$1,BB=$2,CC=$3,DD=$4,TT=$5 WHERE ", sqlCmd)
	assert.Equal(t, []int{0, 2, 4}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "YY=otto AND AA=XXX AND CC=123 AND TT=222", wh)
	wh = createWhere(1, ui, rows)
	assert.Equal(t, "YY=otto AND AA=XXX2 AND CC=51 AND TT=5222", wh)
}
