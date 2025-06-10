package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/types"
)

func ParseSelect (query string, db *types.Database_t) error {
    query = strings.TrimLeft(query, " ")
    searchedColumns, fromIndex, err := findSearchedColumns(query)
    if err != nil {
        return err
    }
    fmt.Println(searchedColumns)

    sourceTable, err := findSourceTable(query, fromIndex)
    if err != nil {
        return err
    }
    fmt.Println(sourceTable)

    i, err := getTableIndex(sourceTable, db)
    if err != nil {
        return err
    }
    fmt.Println("Found index:", i)

    return nil
}



func findSearchedColumns (query string) (searchedColumns []string, fromIndex int, err error) {
    fromIndex = strings.Index(query, "FROM ")
    if fromIndex < 0 {
        return []string{}, -1, errors.New("No \"FROM \" found.")
    }
    searchedColumnsString := query[:fromIndex]
    if len(searchedColumnsString) == 0 {
        return []string{}, -1, errors.New("Must specify columns")
    }
    return strings.Split(strings.ReplaceAll(searchedColumnsString, " ", ""), ","), fromIndex, nil
}



func findSourceTable(query string, fromIndex int) (sourceTableName string, err error) {
    if len(query) <= fromIndex + 5 {
        return "", errors.New("Query does not contain a source table.")
    }
    nextSpace := strings.Index(query[fromIndex+5:], " ")
    if nextSpace < 0 {
        return query[fromIndex+5:], nil
    }
    return query[fromIndex+5:nextSpace], nil
}



func getTableIndex (tableName string, db * types.Database_t) (int, error){
    for i, table := range db.Tables {
    fmt.Println("comparing:", strings.ToUpper(table.Name), "and" ,strings.ToUpper(tableName))
        if strings.ToUpper(table.Name) == strings.ToUpper(tableName){
            return i, nil
        }
    }
    return -1, errors.New(fmt.Sprintf("Table %s does not exist in database %s.", tableName, db.Name))
}


