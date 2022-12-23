package mysql

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mysqlTarget(t *testing.T) (string, error) {
	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlPort := os.Getenv("MYSQL_PORT")
	mysqlPassword := os.Getenv("MYSQL_PWD")
	if !assert.NotEmpty(t, mysqlHost) {
		return "", fmt.Errorf("MySQL Host not set")
	}
	assert.NotEmpty(t, mysqlPort)
	port, err := strconv.Atoi(mysqlPort)
	if !assert.NoError(t, err) {
		return "", fmt.Errorf("MYSQL Port not set")
	}
	mysqlUrl := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", "admin", mysqlPassword, mysqlHost, port, "Bitgarten")

	return mysqlUrl, nil
}

func TestMysqlInit(t *testing.T) {
	url, err := mysqlTarget(t)
	assert.NoError(t, err)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	m, err := pg.Maps()
	assert.NoError(t, err)
	assert.Equal(t, []string{"AlbumPictures", "Albums",
		"PictureLocations", "PictureTag", "PictureTags",
		"Pictures", "TESTTABLEy", "Tags"}, m)
}
