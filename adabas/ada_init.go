package adabas

import (
	"github.com/tknie/adabas-go-api/adabas"
	def "github.com/tknie/db/common"
)

type Adabas struct {
	def.CommonDatabase
	dbURL        string
	dbTableNames []string
}

func New(id def.RegDbID, url string) (def.Database, error) {
	ada := &Adabas{def.CommonDatabase{RegDbID: id}, url, nil}
	err := ada.check()
	if err != nil {
		return nil, err
	}
	return ada, nil
}

func (ada *Adabas) ID() def.RegDbID {
	return ada.RegDbID
}

func (ada *Adabas) URL() string {
	return ada.dbURL
}
func (ada *Adabas) Maps() ([]string, error) {
	return ada.dbTableNames, nil
}

func (ada *Adabas) check() error {
	con, err := adabas.NewConnection(ada.URL())
	if err != nil {
		return err
	}
	defer con.Close()
	listMaps, err := con.GetMaps()
	if err != nil {
		return err
	}
	ada.dbTableNames = listMaps
	return nil
}

func (ada *Adabas) Insert(fields []string, values []any) error {
	return def.NewError(65535)
}

func (ada *Adabas) Query(search *def.Query, f def.ResultFunction) error {
	return def.NewError(65535)
}
