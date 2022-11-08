package postgres

import (
	def "github.com/tknie/db/definition"
)

type PostGres struct {
	def.CommonDatabase
	dbURL string
}

func New(id def.RegDbID, url string) (*PostGres, error) {
	return &PostGres{def.CommonDatabase{RegDbID: id}, url}, nil
}

func (pg *PostGres) ID() def.RegDbID {
	return pg.RegDbID
}

func (pg *PostGres) URL() string {
	return pg.dbURL
}
func (pg *PostGres) Maps() ([]string, error) {
	return make([]string, 0), nil
}
