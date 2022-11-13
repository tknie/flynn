package common

var Databases = make([]Database, 0)

func searchDataDriver(id RegDbID) (Database, error) {
	for _, d := range Databases {
		if d.ID() == id {
			return d, nil
		}
	}
	return nil, NewError(2)
}
