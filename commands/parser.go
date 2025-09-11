package commands

import (
	"errors"
	"fmt"
	"strings"
    "regexp"

	"github.com/MalikL2005/SeliaDB-II/commands/process"
	"github.com/MalikL2005/SeliaDB-II/types"
	"github.com/MalikL2005/SeliaDB-II/commands/parser"
)


func ParseQuery (query string, db *types.Database_t) (numAffectedColumns uint, err error) {
    fmt.Println("parsing:", query)
    query = prepareQuery(query)
    if query == "" {
        return 0,errors.New("Received empty query")
    }
    commandIndex := strings.Index(query, parser.SPACE)
    if commandIndex < 0 {
        return 0, errors.New("invalid query")
    }
    command := GetCommandKeyWord(query[:commandIndex])
    switch command {
    case SELECT:
				values, columns, maxLengths, err := process.SELECT(query[commandIndex:], db)
        if err != nil {
            return 0, err
        }
        numAffectedColumns = 0
				types.DisplayByteSlice(values, columns, maxLengths)
    case INSERT:
        err := process.INSERT(query[commandIndex:], db)
        if err != nil {
            return 0, err
        }
        numAffectedColumns = 1
    case DELETE:
    case UPDATE:
    case NONE: return 0, errors.New(fmt.Sprintf("Unknown command \"%s\". Type help or \\h for more infos.", query))
    }

    return numAffectedColumns, nil
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
    newQuery = replaceSpacesOutsideParenthesis(newQuery)
    return newQuery
}



func replaceSpacesOutsideParenthesis(s string) string {
    fmt.Println("replacing for string")
    re := regexp.MustCompile(`'[^']*'`)
    result := re.ReplaceAllStringFunc(s, func(m string) string {
        // Replace spaces within the matched quoted string
        return strings.ReplaceAll(m, " ", parser.TEMP_SPACE)
    })
    result = strings.ReplaceAll(result, " ", parser.SPACE)
    result = strings.ReplaceAll(result, parser.TEMP_SPACE, " ")

    fmt.Println(result) 
    return result
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

