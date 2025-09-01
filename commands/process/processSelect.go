package process

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	// "github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/commands/parser"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/search"
	"github.com/MalikL2005/SeliaDB-II/types"
)

func SELECT (query string, db *types.Database_t) (values [][][]byte, err error) {
    sourceTb, selectedCols, joinTables, conditions, limit, err := parser.ParseSelect(query, db)
    if err != nil {
        return [][][]byte{}, err
    }

    fmt.Println("selected cols",selectedCols)
    fmt.Println("join tbs", joinTables)
    fmt.Println("conditions", conditions)
    fmt.Println("limit", limit)
    fmt.Println("received", sourceTb, "as table")
    // panic(24234)

    values, currentTb, maxLenghts, colIndices, err := processSelectQuery(db, sourceTb, selectedCols, joinTables, conditions, limit)
    if err != nil {
        return [][][]byte{}, err
    }
    fmt.Println(colIndices)

    fmt.Println(values)
    fmt.Println(selectedCols)
    fmt.Println(joinTables)
    newCols := search.FilterColumns(currentTb.Columns, colIndices)
    fmt.Println(newCols)
    fmt.Println(values)
    types.DisplayByteSlice(values, newCols, maxLenghts)
    return [][][]byte{}, err
}



func processSelectQuery (
				db * types.Database_t,
				sourceTable string, 
				selectedColumns []string,
				joinTables types.Join_t, 
				conditions []types.CompareObj,
				limit uint64) (values [][][]byte, sourceTb *types.Table_t, maxLenghts, colIndices []int, err error){

    fmt.Println("\n\n\nhere:", sourceTable)
    tbIndex, err := getTableIndex(db, sourceTable)
    if err != nil {
        return [][][]byte{}, nil, []int{}, []int{}, err
    }

    currentTb := db.Tables[tbIndex]
    colIndices, err = getColumnIndeces(currentTb, selectedColumns)
    if err != nil {
        return [][][]byte{}, nil, []int{}, []int{}, err
    }
    // cols := filterColumns(currentTb.Columns, colIndices)

    if len(joinTables) > 0 {
        return [][][]byte{}, nil, []int{}, []int{}, errors.New("Join is not implemented yet")
    }

    if len(conditions) == 1 {
        fmt.Println("checking if this col is indexed:", conditions[0].ColName)
				fmt.Println(conditions[0])
        if isIndexed, iCol, err := IsColIndexed(currentTb, conditions[0].ColName); err != nil {
            return [][][]byte{}, nil, []int{}, []int{}, err
        } else if isIndexed {
						fmt.Println(currentTb.Columns[iCol])
						fmt.Println(currentTb.Indeces[iCol])
						root := btree.UnsafePAnyToPNode_t(currentTb.Indeces[iCol].Root) 
						btree.Traverse(root, root)
						entry, err := btree.SearchKey(root, root, conditions[0].Value, currentTb.Columns[iCol].Type)
						if err != nil {
								return [][][]byte{}, nil, []int{}, []int{}, err
						}
						fmt.Println(entry)
						val, _, err := entries.ReadEntryFromFile(currentTb, int(entry.Value))
						if err != nil {
								return [][][]byte{}, nil, []int{}, []int{}, err
						}
						fmt.Println(val)
						newCols := search.FilterColumns(currentTb.Columns, colIndices)
						maxLenghts = types.GetMaxLengthFromBytes(val, newCols)
            return [][][]byte{val}, currentTb, maxLenghts, colIndices, errors.New("Multiple conditions is not implemented yet")
				}
        vals, maxLenghts, err := search.FindEntryWhereCondition(currentTb, colIndices, uint64(limit), conditions...)
        if err != nil {
            return [][][]byte{}, nil, []int{}, []int{}, err
        }
        return vals, currentTb, maxLenghts, colIndices, nil
    }

    if len(conditions) > 1 {
        return [][][]byte{}, nil, []int{}, []int{}, errors.New("Multiple conditions is not implemented yet")
    }
    fmt.Println("\n\n\nvalues:")
    fmt.Println(currentTb)
    fmt.Println()
    if limit <= 0 {
        limit = 10000
    }

    fmt.Println(colIndices)
    
    values, maxLenghts, err = search.IterateOverEntriesInFile(currentTb, colIndices, limit)
    if err != nil {
        return [][][]byte{}, nil, []int{}, []int{}, err
    }

    return values, currentTb, maxLenghts, colIndices, nil
}



func getTableIndex (db * types.Database_t, s string) (int, error) {
    for i, tb := range db.Tables {
        if tb.Name == s {
            return i, nil
        }
    }
    return -1, fmt.Errorf("Table %s does not exist in %s", s, db.Name)
}



func getColumnIndeces (tb *types.Table_t, selectedColumns []string) ([]int, error){
    colNames := make([]string, len(tb.Columns))
    for i, col := range tb.Columns {
        colNames[i] = col.Name
    }

    fmt.Println(selectedColumns)
    fmt.Println("maybe every col?")
    if len(selectedColumns) == 1 && selectedColumns[0] == "*" {
        fmt.Println("every colllll")
        ret := []int{}
        for i := range tb.Columns {
            ret = append(ret, i)
        }
        return ret, nil
    }

    foundColumns := make([]int, 0)
    unknownColumns := make([]string, 0)
    for i, newName := range selectedColumns {
        if slices.Contains(colNames, newName){
            fmt.Println(newName)
            fmt.Println(i)
            foundColumns = append(foundColumns, slices.Index(colNames, newName))
        } else {
            unknownColumns = append(unknownColumns, newName)
        }
    }

    if len(unknownColumns) > 0 {
        return []int{}, fmt.Errorf("Column(s) %s do not exist", strings.Join(unknownColumns[:], ""))
    }
    fmt.Println(foundColumns)
    return foundColumns, nil
}



func IsColIndexed (tb * types.Table_t, colName string) (bool, int, error) {
    colNames := make([]string, len(tb.Columns))
    for i, col := range tb.Columns {
        colNames[i] = col.Name
    }
    if iCol := slices.Index(colNames, colName); iCol == -1 {
        return false, 0, fmt.Errorf("Column %s does not exist in table %s.", colName, tb.Name)
    } else {
        return tb.Columns[iCol].Indexed, iCol, nil
    }
}



