package parser

import (
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/types"
)

func ParseSelect (query string, db *types.Database_t) (sourceTable string, selectedColumns []string, joinTables types.Join_t, conditions []types.CompareObj, limit uint64, err error) {
    fmt.Println("start:", query)
    if query[0:len(SPACE)] != SPACE {
        return "", []string{}, types.Join_t{}, []types.CompareObj{}, 0, errors.New("Expected SELECT and then space.")
    }
    // columns
    var curIndex int
    selectedColumns, curIndex, err = findSearchedColumns(query)
    if err != nil {
        return "", []string{}, types.Join_t{}, []types.CompareObj{}, 0, errors.New(fmt.Sprint("ParseSelect #01", err))
    } 
    fmt.Println("searched cols:", selectedColumns)
    fmt.Println("after searched cols:", query[curIndex:])


    // table
    sourceTable, curIndex, err = findSourceTable(query, curIndex)
    if err != nil {
        return "", []string{}, types.Join_t{}, []types.CompareObj{}, 0, errors.New(fmt.Sprint("ParseSelect #02", err))
    }
    fmt.Println("source tb:", sourceTable)

    i, err := getTableIndex(sourceTable, db)
    if err != nil {
        return "", []string{}, types.Join_t{}, []types.CompareObj{}, 0, errors.New(fmt.Sprint("ParseSelect #03", err))
    }
    fmt.Println("Found index:", i)

    if curIndex < 0 {
        fmt.Println("We are done here -> select ... from x;")
        return sourceTable, selectedColumns, types.Join_t{}, []types.CompareObj{}, 0, nil
    }
    fmt.Println("after selected cols:", query[curIndex:])

    // join tables
    joinTables, curIndex, err = getJoinTables(query)
    if err != nil {
        return "", []string{}, types.Join_t{}, []types.CompareObj{}, 0, errors.New(fmt.Sprint("ParseSelect #04", err))
    }
    fmt.Println("\n\n\n\nJoin tables:", joinTables)
    fmt.Println(query)
    if strings.HasPrefix(query[curIndex:], SPACE) {
        curIndex += len(SPACE)
    }
    fmt.Println("after joinTables:", query[curIndex:])
    fmt.Println(query)
    // where conditions
    compareObjs, plusIndex, err := getWhereConditions(query[curIndex:])
    if err != nil {
        return "", []string{}, types.Join_t{}, []types.CompareObj{}, 0, errors.New(fmt.Sprint("ParseSelect #05", err))
    }
    curIndex += plusIndex
    fmt.Println(compareObjs)
    
    if curIndex >= len(query) {
        fmt.Println(query)
        fmt.Println(curIndex)
        return sourceTable, selectedColumns, joinTables, compareObjs, 0, errors.New("herlllp")
    }
    fmt.Println("after conditions:", query[curIndex:])

    // limit
    fmt.Println("\n\n\n", query[curIndex:])
    limit, err = getLimit(query[curIndex:])
    if err != nil {
        return sourceTable, selectedColumns, joinTables, compareObjs, 0, err
    }
    fmt.Println("after limit:", query[curIndex:])
    fmt.Println(limit)
    fmt.Println(query)
    fmt.Println(query[curIndex:])

    return sourceTable, selectedColumns, joinTables, compareObjs, limit, nil
}



func findSearchedColumns (query string) (searchedColumns []string, curIndex int, err error) {
    curIndex = strings.Index(query, "FROM"+SPACE)
    if curIndex < 0 {
        return []string{}, -1, errors.New("No \"FROM \" found.")
    }
    return strings.Split(strings.ReplaceAll(query[:curIndex], SPACE, ""), ","), curIndex, nil
}



func findSourceTable(query string, fromIndex int) (sourceTableName string, curIndex int, err error) {
    FromLength := len("FROM"+SPACE)
    if len(query) <= fromIndex + FromLength {
        return "", -1, errors.New("Query does not contain a source table.")
    }
    nextSpace := strings.Index(query[fromIndex+FromLength:], SPACE)
    if nextSpace < 0 {
        return query[fromIndex+FromLength:], nextSpace, nil
    }
    return query[fromIndex+FromLength:fromIndex+FromLength+nextSpace], fromIndex+FromLength+nextSpace, nil
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



func getJoinTables (query string) (joinTables types.Join_t, plusIndex int, err error){
    joinTables = types.Join_t{}
    for {
        joinIndex := strings.Index(query, "JOIN"+SPACE)
        if joinIndex <0 {
            fmt.Println("No more join tables")
            break
        }
        query = query[joinIndex+len("JOIN"+SPACE):]
        plusIndex += joinIndex+len("JOIN"+SPACE)
        // check for type of join (inner, outer, etc.) here

        nextSpace := strings.Index(query, SPACE)
        if nextSpace < 0 {
            return types.Join_t{}, 0, errors.New("Expected <space>")
        }
        plusIndex += nextSpace
        joinTableName := query[:nextSpace]
        fmt.Println("New join table:", joinTableName)
        query = query[nextSpace:]
        if strings.HasPrefix(query, SPACE){
            plusIndex += len(SPACE)
            query = query[len(SPACE):]
        }
        fmt.Println(query)
        if query[:len("ON"+SPACE)] != "ON"+SPACE {
            return types.Join_t{}, 0, errors.New("Must specify ON-columns")
        }
        
        query = query[len("ON"+SPACE):]
        plusIndex += len("ON"+SPACE)
        fmt.Println(query)
        nextEq := strings.Index(query, "=")
        if nextEq <= 0 {
            return types.Join_t{}, 0, errors.New("Must specify =")
        }
        leftJoinColumn := query[:nextEq]
        query = query[nextEq+1:]
        plusIndex += nextEq+1
        nextSpace = strings.Index(query, SPACE)
        var rightJoinColumn string
        if nextSpace > 0 {
            rightJoinColumn = query[:nextSpace]
            query = query[nextSpace+len(SPACE):]
            plusIndex += nextSpace+len(SPACE)
        } else {
            fmt.Println("current query:", query)
            // rightJoinColumn = strings.TrimLeft(query[nextEq+1:], " ")
            rightJoinColumn = query
        }
        fmt.Println("ljc",leftJoinColumn)
        fmt.Println("rjc", rightJoinColumn)
        joinTables[joinTableName] = struct {Left string; Right string; How types.JoinType}{
            Left: leftJoinColumn,
            Right: rightJoinColumn,
            How: types.GetJoinType("INNER"),
        }
        fmt.Println(query)
    }
    return joinTables, plusIndex, nil
}




func getWhereConditions(query string) (cmpObjs []types.CompareObj, curIndex int, err error){
    fmt.Println(query)
    fmt.Println(len(query))
    if len(query) <= len(WHERE + SPACE) || query[:len(WHERE+SPACE)] != WHERE+SPACE {
        return []types.CompareObj{}, 0, nil
    }
    curIndex = len("WHERE"+SPACE)
    var nextSpace int
    curConnector := types.AND
    for {
        fmt.Println("parsing conditon(s)")
        nextOperator, startOffsetToNO, endOffsetToNO, err := findNextCompareOperator(query, curIndex)
        if err != nil {
            fmt.Println("broke out here")
            fmt.Println(query[curIndex:])
            break
        }
        var compareVal string
        if query[curIndex+endOffsetToNO] == '\'' {
            nextSpace = strings.Index(query[curIndex+endOffsetToNO+1:], "'")
            compareVal = query[curIndex+endOffsetToNO:curIndex+endOffsetToNO+nextSpace+2] // +2 because of the ''
        } else {
            nextSpace = strings.Index(query[curIndex+endOffsetToNO:], SPACE)
            if nextSpace < 0 {
                compareVal = query[curIndex+endOffsetToNO:] // +2 because of the ''
            } else {
                compareVal = query[curIndex+endOffsetToNO:curIndex+endOffsetToNO+nextSpace] // +2 because of the ''
            }
        }
        compareCol := strings.TrimLeft(query[curIndex:curIndex+startOffsetToNO], SPACE)

        cmpObjs = append(cmpObjs, types.CompareObj{
            ColName: compareCol,
            Value: compareVal,
            CmpOperator: types.GetCompareOperator(nextOperator),
            Connector: curConnector,
        })
        fmt.Println("got:", compareCol, nextOperator, compareVal)

        fmt.Println(query[curIndex+len(compareVal):])
        if strings.HasPrefix(compareVal, "'"){
            curIndex += endOffsetToNO + len(compareVal + SPACE)
        } else {
            if nextSpace = strings.Index(query[curIndex+endOffsetToNO:], SPACE); nextSpace <= 0 {
                curIndex = len(query)-1
                break
            } else {
                curIndex += endOffsetToNO + nextSpace
            }
        }
        fmt.Println(query)
        if curIndex >= len(query){
            break
        }
        if strings.HasPrefix(query[curIndex:], SPACE){
            curIndex += len(SPACE)
        }
        curConnector, nextSpace = CheckForContinueConditions(query[curIndex:])
        fmt.Println(query[curIndex:])
        fmt.Println("nextsplace:", nextSpace)
        curIndex += nextSpace
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
    if strings.HasPrefix(queryPart, OR+SPACE){
        return types.OR, strings.Index(queryPart, OR+SPACE) + len(OR+SPACE)
    }
    if strings.HasPrefix(queryPart, AND+SPACE){
        return types.AND, strings.Index(queryPart, AND+SPACE) + len(AND+SPACE)
    }
    return types.MISSING_CONNECTOR, 0
}



func getLimit(query string) (uint64, error) {
    fmt.Println("In get limit:", query)
    if strings.HasPrefix(strings.TrimLeft(query, SPACE), LIMIT+SPACE){
        num, err := strconv.ParseUint(strings.TrimLeft(query, SPACE)[len(LIMIT+SPACE):], 10, 64)
        if err != nil {
            return 0, err
        }
        return num, nil
    }
    if len(query) <= len(LIMIT){
        return 0, nil
    }
    num, err := strconv.ParseUint(query[len(LIMIT):], 10, 64)
    if err != nil {
        return 0, err
    }
    return num, nil
}



