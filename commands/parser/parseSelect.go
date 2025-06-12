package parser

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/types"
)

func ParseSelect (query string, db *types.Database_t) error {
    query = strings.TrimLeft(query, " ")
    // columns
    searchedColumns, curIndex, err := findSearchedColumns(query)
    if err != nil {
        return err
    }
    fmt.Println(searchedColumns)

    // table
    sourceTable, curIndex, err := findSourceTable(query, curIndex)
    if err != nil {
        return err
    }
    fmt.Println(sourceTable)
    i, err := getTableIndex(sourceTable, db)
    if err != nil {
        return err
    }
    fmt.Println("Found index:", i)

    if curIndex < 0 {
        fmt.Println("We are done here -> select * from x;")
        return nil
    }

    // join tables
    joinedTables, curIndex, err := getJoinTables(query, curIndex)
    if err != nil {
        return err
    }
    fmt.Println(joinedTables)

    // where conditions
    compareObjs, curIndex, err := getWhereConditions(query, curIndex)
    if err != nil {
        return err
    }
    fmt.Println(compareObjs)

    fmt.Println(curIndex)
    fmt.Println(query[curIndex:])

    return nil
}



func findSearchedColumns (query string) (searchedColumns []string, curIndex int, err error) {
    curIndex = strings.Index(query, "FROM ")
    if curIndex < 0 {
        return []string{}, -1, errors.New("No \"FROM \" found.")
    }
    searchedColumnsString := query[:curIndex]
    if len(searchedColumnsString) == 0 {
        return []string{}, -1, errors.New("Must specify columns")
    }
    return strings.Split(strings.ReplaceAll(searchedColumnsString, " ", ""), ","), curIndex, nil
}



func findSourceTable(query string, fromIndex int) (sourceTableName string, curIndex int, err error) {
    if len(query) <= fromIndex + 5 { // +5 because len("FROM ") == 5
        return "", -1, errors.New("Query does not contain a source table.")
    }
    nextSpace := strings.Index(query[fromIndex+5:], " ")
    if nextSpace < 0 {
        return query[fromIndex+5:], nextSpace, nil
    }
    return query[fromIndex+5:fromIndex+5+nextSpace], fromIndex+5+nextSpace, nil
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



func getJoinTables (query string, startIndex int) (joinTables map[string]struct {
    Left string
    Right string},
    curIndex int, err error){
    joinColumns := make(map[string]struct {Left string; Right string})
    for {
        query = strings.TrimLeft(query, " ")
        joinIndex := strings.Index(query, "JOIN ")
        if joinIndex <0 {
            fmt.Println("No join tables")
            break
        }
        query = query[joinIndex+5:]
        // check for type of join (inner, outer, etc.) here

        nextSpace := strings.Index(query, " ")
        if nextSpace < 0 {
            return map[string]struct{Left string; Right string}{}, startIndex, errors.New("Expected <space>")
        }
        joinTable := query[:nextSpace]
        fmt.Println("New join table:", joinTable)
        query = strings.TrimLeft(query[nextSpace:], " ")
        fmt.Println(query[:3])
        if query[:3] != "ON " {
            return map[string]struct{Left string; Right string}{}, startIndex, errors.New("Must specify ON-columns")
        }
        
        query = strings.TrimLeft(query[2:], " ")
        fmt.Println(query)
        nextEq := strings.Index(query, "=")
        leftJoinColumn := strings.Trim(query[:nextEq], " ")
        query = strings.TrimLeft(query[nextEq+1:], " ")
        nextSpace = strings.Index(query, " ")
        var rightJoinColumn string
        if nextSpace > 0 {
            rightJoinColumn = query[:nextSpace]
            query = strings.TrimLeft(query[nextSpace:], " ")
        } else {
            fmt.Println("current query:", query)
            // rightJoinColumn = strings.TrimLeft(query[nextEq+1:], " ")
            rightJoinColumn = strings.TrimLeft(query, " ")
        }
        fmt.Println("ljc",leftJoinColumn)
        fmt.Println("rjc", rightJoinColumn)
        joinColumns[joinTable] = struct{Left string; Right string}{
            Left: leftJoinColumn,
            Right: rightJoinColumn,
        }
    }
    return joinColumns, startIndex, nil
}




func getWhereConditions(query string, startIndex int) (cmpObjs []types.CompareObj, curIndex int, err error){
    fmt.Println(query[startIndex:])
    fmt.Printf("'%s'\n",strings.TrimLeft(query[startIndex:startIndex+7], " "))
    if strings.TrimLeft(query[startIndex:startIndex+7], " ") != "WHERE " {
        return []types.CompareObj{}, startIndex, nil
    }
    fmt.Println(query[startIndex+7])
    curIndex = startIndex +7
    var nextSpace int
    curConnector := types.AND
    for {
        fmt.Println("parsing conditon(s)")
        nextOperator, startOffsetToNO, endOffsetToNO, err := findNextCompareOperator(query, curIndex)
        if err != nil {
            // return []types.CompareObj{}, curIndex, err
            break
        }
        fmt.Println(nextOperator, startOffsetToNO, endOffsetToNO, err)
        fmt.Println("data:")
        fmt.Println(query[curIndex+startOffsetToNO:])
        var compareVal string
        if query[curIndex+endOffsetToNO] == '\'' {
            fmt.Println("hererere")
            nextSpace = strings.Index(query[curIndex+endOffsetToNO+1:], "'")
            compareVal = query[curIndex+endOffsetToNO:curIndex+endOffsetToNO+nextSpace+2] // +2 because of the ''
        } else {
            nextSpace = strings.Index(query[curIndex+endOffsetToNO:], " ")
            if nextSpace < 0 {
                compareVal = query[curIndex+endOffsetToNO:] // +2 because of the ''
            } else {
                compareVal = query[curIndex+endOffsetToNO:curIndex+endOffsetToNO+nextSpace] // +2 because of the ''
            }
            fmt.Println("current:", query[curIndex+endOffsetToNO:])
            fmt.Println("now:", compareVal)
        }
        compareCol := query[curIndex:curIndex+startOffsetToNO]

        cmpObjs = append(cmpObjs, types.CompareObj{
            ColName: compareCol,
            Value: compareVal,
            CmpOperator: types.GetCompareOperator(nextOperator),
            Connector: curConnector,
        })
        fmt.Println("\n\ngot:\n", compareCol, nextOperator, compareVal, "\n")

        fmt.Println(query[curIndex+len(compareVal):])
        if strings.HasPrefix(compareVal, "'"){
            curIndex += endOffsetToNO + len(compareVal) +1 // +1 for " "
        } else {
            curIndex += endOffsetToNO + strings.Index(query[curIndex:], " ") +1 // +1 for " "
        }
        curConnector, nextSpace = CheckForContinueConditions(query[curIndex:])
        curIndex += nextSpace
        fmt.Println("conn", curConnector)
        fmt.Println("continuing with:", query[curIndex+nextSpace:])
    }
    return cmpObjs, curIndex, nil
}


func findNextCompareOperator (query string, curIndex int) (nextOperator string, startOffsetToNO, endOffsetToNO int, err error) {
    fmt.Println(query[curIndex:])
    smallest := struct {operator string; num int}{}
    for operator := range maps.Keys(types.CompareStrings){
        // todo: check for >= before >
        n := strings.Index(query[curIndex:], operator)
        if n>0 && (smallest.num > n || smallest.num == 0) {
            smallest.operator = operator
            smallest.num = n
        }
    }
    fmt.Println(smallest)
    if smallest.operator == ""{
        return "", -1, -1, errors.New("No operator found.")
    }
    return smallest.operator, smallest.num, smallest.num + len(smallest.operator), nil
}




func CheckForContinueConditions (queryPart string) (nextconnector types.CompareConnector, indexAfterConnector int) {
    q1 := strings.TrimLeft(queryPart, " ")
    if strings.HasPrefix(q1, "OR "){
        return types.OR, strings.Index(queryPart, "OR ") + 3
    }
    if strings.HasPrefix(q1, "AND "){
        return types.AND, strings.Index(queryPart, "AND ") + 4
    }
    return types.MISSING_CONNECTOR, 0
}



