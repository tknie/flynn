package db

import "fmt"

type Database interface {
	URL() string
	Maps() ([]string, error)
}

var databases = make([]Database, 0)

func Register(typeName, url string) (bool, error) {
	return false, nil
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
