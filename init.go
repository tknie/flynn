package db

import (
	"fmt"
	"sync/atomic"

	def "github.com/tknie/db/definition"
	"github.com/tknie/db/postgres"
)

type Database interface {
	ID() def.RegDbID
	URL() string
	Maps() ([]string, error)
}

var globalRegID = def.RegDbID(0)

var databases = make([]Database, 0)

func Register(typeName, url string) (def.RegDbID, error) {
	id := def.RegDbID(atomic.AddUint64((*uint64)(&globalRegID), 1))

	var db Database
	var err error
	switch typeName {
	case "postgres":
		db, err = postgres.New(id, url)
		if err != nil {
			return 0, err
		}
	}
	databases = append(databases, db)
	return db.ID(), nil
}

func Maps() []string {
	databaseMaps := make([]string, 0)
	for _, database := range databases {
		fmt.Println(database.URL())
		subMaps, err := database.Maps()
		if err != nil {
			fmt.Println("Error reading sub maps:", err)
		}
		databaseMaps = append(databaseMaps, subMaps...)
	}
	return databaseMaps
}

func Unregister(id def.RegDbID) error {
	for i, d := range databases {
		fmt.Println(id, "check to", d.ID())
		if d.ID() == id {
			newDatabases := make([]Database, 0)
			if i > 0 {
				newDatabases = append(newDatabases, databases[0:i-1]...)
			}
			if len(databases)-1 > i {
				newDatabases = append(newDatabases, databases[i+1:]...)
			}
			databases = newDatabases
			return nil
		}
	}
	return fmt.Errorf("ID not found")
}
