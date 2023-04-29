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

```go
 pg := "postgres://pguser:<pass>@pghost:5432/pgdatabase"
 x, err := Register("postgres", pg)
 if err!=nil {
  return
 }
 defer Unregister(x)

 q := &common.Query{TableName: "Albums",
  Search: "id=23",
  Fields: []string{"Title", "created"}}
 counter := 0
 _, err = x.Query(q, func(search *common.Query, result *common.Result) error {
                fmt.Println(*(result.Rows[0].(*string)),ns := *(result.Rows[1].(*string)))
                return nil
            })
```

### Update records in database

```go
 pg := "postgres://pguser:<pass>@pghost:5432/pgdatabase"
 if err!=nil {
  return
 }

 x, err := Register("postgres", pg)
 if err!=nil {
  return
 }
 defer Unregister(x)

 list := [][]any{{"ABC","AAA",1,2,3}}
 err = x.Update(testStructTable, 
       &common.Entries{Fields: []string{"ID", "Name","Value1","Value2","Value3"}, Values: list})
 if err!=nil {
  return
 }
```

## Check List

Feature | Ready-State | Description
---------|----------|---------
 Query PostgreSQL | :heavy_check_mark: | Draft
 Search PostgreSQL | :heavy_check_mark: | Draft
 Create table PostgreSQL | :heavy_check_mark: | Draft
 Insert PostgreSQL | :heavy_check_mark: | Draft
 Update PostgreSQL | :heavy_check_mark: | Draft
 Query MySQL | :heavy_check_mark: | Draft
 Search MySQL | :heavy_check_mark: | Draft
 Create table MySQL | :heavy_check_mark: | Draft
 Insert MySQL | :heavy_check_mark: | Draft
 Update MySQL | :heavy_check_mark: | Draft
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
