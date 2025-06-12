package commands

import (
	"errors"
	"fmt"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/commands/parser"
	"github.com/MalikL2005/SeliaDB-II/types"
)


func ParseQuery (query string, db *types.Database_t) (error) {
    fmt.Println("parsing:", query)
    query = prepareQuery(query)
    if query == "" {
        return errors.New("Received empty query")
    }
    commandIndex := strings.Index(query, " ")
    if commandIndex < 0 {
        return errors.New("invalid query")
    }
    command := GetCommandKeyWord(query[:commandIndex])
    switch command {
    case SELECT:
        err := parser.ParseSelect(query[commandIndex:], db)
        if err != nil {
            return err
        }
        case INSERT:
        case DELETE:
        case UPDATE:
        case NONE: return errors.New(fmt.Sprintf("Unknown command \"%s\". Type help or \\h for more infos.", query))
    }

    return nil
}



func prepareQuery (oldQuery string) (newQuery string) {
    newQuery = strings.Join(strings.Split(strings.Trim(oldQuery, " "), " "), " ")
    newQuery = strings.ReplaceAll(newQuery, " from ", " FROM ")
    newQuery = strings.ReplaceAll(newQuery, " where ", " WHERE ")
    newQuery = strings.ReplaceAll(newQuery, " and ", " AND ")
    newQuery = strings.ReplaceAll(newQuery, " limit ", " LIMIT ")
    newQuery = strings.ReplaceAll(newQuery, "select ", "SELECT ")
    newQuery = strings.ReplaceAll(newQuery, "insert ", "INSERT ")
    newQuery = strings.ReplaceAll(newQuery, "delete ", "DELETE ")
    newQuery = strings.ReplaceAll(newQuery, "update ", "UPDATE ")
    newQuery = strings.ReplaceAll(newQuery, " join ", " JOIN ")
    newQuery = strings.ReplaceAll(newQuery, " on ", " ON ")
    newQuery = strings.ReplaceAll(newQuery, " inner ", " INNER ")
    newQuery = strings.ReplaceAll(newQuery, " left ", " LEFT ")
    newQuery = strings.ReplaceAll(newQuery, " right ", " RIGHT ")
    newQuery = strings.ReplaceAll(newQuery, " outer ", " OUTER ")
    newQuery = strings.ReplaceAll(newQuery, " as ", " AS ")
    newQuery = strings.ReplaceAll(newQuery, " < ", "<")
    newQuery = strings.ReplaceAll(newQuery, " > ", ">")
    newQuery = strings.ReplaceAll(newQuery, " = ", "=")
    newQuery = strings.ReplaceAll(newQuery, " <= ", "<=")
    newQuery = strings.ReplaceAll(newQuery, " >= ", ">=")
    newQuery = strings.ReplaceAll(newQuery, " != ", "!=")
    return newQuery
}





/*
SELECT <columns> FROM <table> 
    JOIN <othertable> 
    WHERE <condition> 
    LIMIT <limit>

INSERT INTO <table>
    (<columns>)
    VALUES ()

DELETE FROM <table>
    WHERE <condition>

update <columns> IN <table> 
    VALUES ()
*/

