package oracle

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

var logRus = logrus.StandardLogger()
var once = new(sync.Once)

func InitLog(t *testing.T) {
	once.Do(startLog)
	log.Log.Debugf("TEST: %s", t.Name())
}

func startLog() {
	fmt.Println("Init logging")
	fileName := "oracle.test.log"
	level := os.Getenv("ENABLE_DB_DEBUG")
	logLevel := logrus.WarnLevel
	switch level {
	case "debug", "1":
		log.SetDebugLevel(true)
		logLevel = logrus.DebugLevel
	case "info", "2":
		log.SetDebugLevel(false)
		logLevel = logrus.InfoLevel
	default:
	}
	logRus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05",
	})
	logRus.SetLevel(logLevel)
	p := os.Getenv("LOGPATH")
	if p == "" {
		p = os.TempDir()
	}
	f, err := os.OpenFile(p+"/"+fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening log:", err)
		return
	}
	logRus.SetOutput(f)
	logRus.Infof("Init logrus")
	log.Log = logRus
	fmt.Println("Logging running")
}

func TestOracleURL(t *testing.T) {
	o, err := NewInstance(common.RegDbID(1),
		&common.Reference{Driver: common.OracleType, Host: "abc",
			Port: 12345, Database: "SchemaXXX"}, "AA")
	assert.NoError(t, err)
	assert.Equal(t, "user=\"<user>\" password=\"<password>\" connectString=\"(DESCRIPTION =(ADDRESS_LIST =(ADDRESS =(PROTOCOL = TCP)(HOST = abc)(PORT = 12345)))(CONNECT_DATA=(SERVICE_NAME = SchemaXXX))\"", o.URL())

	ref, pwd, err := common.NewReference("oracle://user='admin' password='yppsfdasfds1#' connectstring='@DESCRIPTION =(ADDRESS_LIST =(ADDRESS =(PROTOCOL = TCP)(HOST = abc)(PORT = 12345)))(CONNECT_DATA=(SERVICE_NAME = SchemaXXX))'")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "admin", ref.User)
	assert.Equal(t, "yppsfdasfds1#", pwd)
	assert.Equal(t, common.OracleType, ref.Driver)
	assert.Equal(t, "@DESCRIPTION =(ADDRESS_LIST =(ADDRESS =(PROTOCOL = TCP)(HOST = abc)(PORT = 12345)))(CONNECT_DATA=(SERVICE_NAME = SchemaXXX))", ref.Options[0])
}

func oracleTable(t *testing.T) string {
	url := os.Getenv("ORACLE_URL")
	// assert.NotEmpty(t, url)
	return url
}

func TestOracleMaps(t *testing.T) {
	InitLog(t)

	url := oracleTable(t)
	if url == "" {
		return
	}
	ref, passwd, err := common.NewReference(url)
	assert.NotEmpty(t, passwd)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, ref)

	ora, err := NewInstance(1, ref, passwd)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, ora)

	list, err := ora.Maps()
	assert.NoError(t, err)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, len(list) > 0)
	fmt.Println("List", list)
}

func TestOracleRead(t *testing.T) {
	InitLog(t)

	url := oracleTable(t)
	if url == "" {
		return
	}
	tablename := os.Getenv("ORACLE_TABLE")
	search := os.Getenv("ORACLE_SEARCH")
	fields := strings.Split(os.Getenv("ORACLE_FIELDS"), ",")
	ref, passwd, err := common.NewReference(url)
	assert.NotEmpty(t, passwd)
	assert.NoError(t, err)
	if !assert.NotNil(t, ref) {
		return
	}

	ora, err := NewInstance(1, ref, passwd)
	assert.NoError(t, err)
	if !assert.NotNil(t, ora) {
		return
	}
	if !assert.NotEmpty(t, ora.(*Oracle).dbURL) {
		return
	}

	q := &common.Query{TableName: tablename, Search: search, Fields: fields}
	_, err = ora.Query(q, func(search *common.Query, result *common.Result) error {
		for _, d := range result.Rows {
			switch v := d.(type) {
			case *sql.NullString:
				if v.Valid {
					fmt.Print(v.String)
				} else {
					fmt.Print("<nil>")
				}
			case *sql.NullInt64:
				if v.Valid {
					fmt.Print(v.Int64)
				} else {
					fmt.Print("<nil>")
				}
			case *sql.NullFloat64:
				if v.Valid {
					fmt.Print(v.Float64)
				} else {
					fmt.Print("<nil>")
				}
			default:
				fmt.Print(d)
			}
			fmt.Print("|")
		}
		fmt.Println()
		return nil
	})
	assert.NoError(t, err)
}
