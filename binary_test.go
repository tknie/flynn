package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/db/common"
	def "github.com/tknie/db/common"
)

func init() {
	err := initLog("binary.log")
	if err != nil {
		fmt.Println("ERROR : ", err)
		return
	}

}

func TestBinarySearchPgRows(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	common.Log.Debugf("Binary test")

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Pictures",
		Search: "ChecksumPicture='B64F1DDF5683608579998E618545E497        '",
		Fields: []string{"Thumbnail"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 1)
		image := *(result.Rows[0].(*[]byte))
		counter++
		switch counter {
		case 1:
			assert.Len(t, image, 10074)
		default:
			assert.NotEqual(t, "blabla", image)
		}

		return nil
	})
	assert.NoError(t, err)
}
