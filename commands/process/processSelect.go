package process

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/commands/parser"
	"github.com/MalikL2005/SeliaDB-II/search"
	"github.com/MalikL2005/SeliaDB-II/types"
)

func SELECT (query string, db *types.Database_t) (values [][][]byte, err error) {
    sourceTb, selectedCols, joinTables, conditions, err := parser.ParseSelect(query, db)
    if err != nil {
        return [][][]byte{}, err
    }

    fmt.Println("received", sourceTb, "as table")

    values, currentTb, maxLenghts, err := processSelectQuery(db, sourceTb, selectedCols, joinTables, conditions)
    if err != nil {
        return [][][]byte{}, err
    }

    types.DisplayByteSlice(values, currentTb, maxLenghts)
    return [][][]byte{}, err
}

func processSelectQuery (
    db * types.Database_t,
    sourceTable string, 
    selectedColumns []string,
    joinTables types.Join_t, 
    conditions []types.CompareObj) (values [][][]byte, sourceTb *types.Table_t, maxLenghts []int, err error){

    fmt.Println("\n\n\nhere:", sourceTable)
    tbIndex, err := getTableIndex(db, sourceTable)
    if err != nil {
        return [][][]byte{}, nil, []int{}, err
    }

    currentTb := db.Tables[tbIndex]
    // indices, err := getColumnIndeces(currentTb, selectedColumns)
    // if err != nil {
    //     return [][][]byte{}, err
    // }

    if len(joinTables) > 0 {
        return [][][]byte{}, nil, []int{}, errors.New("Join is not implemented yet")
    }

    if len(conditions) > 0 {
        return [][][]byte{}, nil, []int{}, errors.New("Condition is not implemented yet")
    }
    fmt.Println("\n\n\nvalues:")
    fmt.Println(currentTb)
    fmt.Println()
    
    values, maxLenghts, err = search.IterateOverEntriesInFile(currentTb, 10000)
    if err != nil {
        return [][][]byte{}, nil, []int{}, err
    }

    return values, currentTb, maxLenghts, nil
}



func getTableIndex (db * types.Database_t, s string) (int, error) {
    for i, tb := range db.Tables {
        if tb.Name == s {
            return i, nil
        }
    }
    return -1, errors.New(fmt.Sprintf("Table %s does not exist in %s", s, db.Name))
}



func getColumnIndeces (tb *types.Table_t, selectedColumns []string) ([]int, error){
    colNames := make([]string, len(tb.Columns))
    for i, col := range tb.Columns {
        colNames[i] = col.Name
    }

    foundColumns := make([]int, len(tb.Columns))
    unknownColumns := make([]string, 0)
    for i, newName := range selectedColumns {
        if slices.Contains(colNames, newName){
            foundColumns[i] = slices.Index(colNames, newName)
        } else {
            unknownColumns = append(unknownColumns, newName)
        }
    }

    if len(unknownColumns) > 0 {
        return []int{}, errors.New(fmt.Sprintf("Column(s) %s do not exist", strings.Join(unknownColumns[:], "")))
    }
    return foundColumns, nil
}



