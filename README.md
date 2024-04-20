# Flynn DB layer

## Introduction

The `github.com/tknie/flynn` package contains an inbetween layer to access multiple types of databases with the same API. Move to other database types would not lead to change the inbetween layer.
Copying of data between different database engine should be possible.

There should be no difference if the database is a SQL or a NoSQL database.

A list of data recrods should be able to be inserted or updated in one call. May be as transaction or in an atomic matter.

In advance real main database functionality should be contained like:

* Create database tables
* Read, Search, Insert, Update and Delete of data records
* Search by passing database specific queries to be flexible for complex queries
* One-to-One mapping of `Golang` structures to database tables
* Large object support of big data
* Support creating batch jobs for database-specific tasks like SQL scripts
* Create index or other enhancements on database configuration

For details have a look at the API documentation. It can be referenced here: <https://godoc.org/github.com/tknie/flynn>

## Example of Flynn usage

### Query records in database

#### Using row queries to get out of result records

```go
 pg := "postgres://pguser:<pass>@pghost:5432/pgdatabase"
 id, err := flynn.Handle("postgres", pg)
 if err!=nil {
  return
 }
 defer id.FreeHandler()

 q := &common.Query{TableName: "Albums",
  Search: "id=23",
  Fields: []string{"Title", "created"}}
 _, err = id.Query(q, func(search *common.Query, result *common.Result) error {
                fmt.Println(*(result.Rows[0].(*string)),ns := *(result.Rows[1].(*string)))
                return nil
            })
```

#### Using GO struct queries to get data directly in GO structures

It is possible to use GO structs to query database directly into instance value. The corresponding entry is provided in the `result.Data` field. The corresponding field entry is reused and not recreated for each function call. The query can be restricted to some fields of the structure only using the `Fields` field in the `common.Query` definition. Field names can be overriden using the field tag `flynn:` definition.

Here is an example query using GO structures:

```go
type Employee struct {
	FirstName  string `flynn:"first_name"`
	Name       string `flynn:"last_name"`
	Department string
	Birth      time.Time
}

employee := &Employee{}

userURL := "postgres://user:abc@postgreshost:5432/mydb"
userDbRef, _, err := common.NewReference(userURL)
if err!=nil {
	return
}
postgresPassword := os.Getenv("POSTGRES_PWD")
id, err := flynn.Handler(userDbRef, postgresPassword)
if err!=nil {
	return
}
defer id.FreeHandler()

q := &common.Query{TableName: "Employees",
	DataStruct: employee,
	Search:     "id=23",
	Fields:     []string{"*"}}
_, err = id.Query(q, func(search *common.Query, result *common.Result) error {
	e := result.Data.(*Employee)
	fmt.Println(e.FirstName, " ", e.Name, " ", e.Birth)
	return nil
})
```

### Update records in database

The update and insert are using the corresponding `common.Entries` structure to define the update or insert. Similar to queries a GO structure can be used for an update.

```go
 pg := "postgres://pguser:<pass>@pghost:5432/pgdatabase"
 if err!=nil {
  return
 }

 x, err := flynn.Handle("postgres", pg)
 if err!=nil {
  return
 }
 defer x.FreeHandler()

 list := [][]any{{"ABC","AAA",1,2,3}}
 _,err = x.Update(testStructTable, 
       &common.Entries{Fields: []string{"ID", "Name","Value1","Value2","Value3"}, Values: list})
 if err!=nil {
  return
 }
```

## Check List

Feature | Ready-State | Description
---------|----------|---------
  **PostgreSQL** || 
 Query PostgreSQL | :heavy_check_mark: | Draft
 Search PostgreSQL | :heavy_check_mark: | Draft
 Create table PostgreSQL | :heavy_check_mark: | Draft
 Insert PostgreSQL | :heavy_check_mark: | Draft
 Update PostgreSQL | :heavy_check_mark: | Draft
  **MySQL**/**Oracle** || 
 Query MySQL | :heavy_check_mark: | Draft
 Search MySQL | :heavy_check_mark: | Draft
 Create table MySQL | :heavy_check_mark: | Draft
 Insert MySQL | :heavy_check_mark: | Draft
 Update MySQL | :heavy_check_mark: | Draft
  **Adabas** || 
 Query Adabas | :heavy_check_mark: | Draft
 Search Adabas | | Draft
 Create table Adabas | Not possible | 
 Insert Adabas |  | Draft
 Update Adabas |  | Not implemented
 Work with large objects (LOB) |  | partial done
 Work with database-specific queries |  | planned
 Use Golang structure with query | partial done | MySQL and PostgresSQL
 Function-based query | | Used during search and query
 Support creating batch jobs for database-specific tasks like SQL scripts | | partial done
 Create index or other enhancements on database configuration | | planned
 Enhanced Search topics || planned
 Common search queries (common to SQL or NonSQL databases) |  | planned
 Use globale transaction (combine update and insert) | partial done | MySQL and PostgresSQL
