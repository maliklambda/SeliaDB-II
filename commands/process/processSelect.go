package process

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/commands/parser"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/joins"
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

    values, cols, maxLenghts, err := processSelectQuery(db, sourceTb, selectedCols, joinTables, conditions, limit)
    if err != nil {
        return [][][]byte{}, err
    }
    fmt.Println(cols)

    fmt.Println(values)
    fmt.Println(selectedCols)
    fmt.Println(joinTables)
    fmt.Println(maxLenghts)
    // types.DisplayByteSlice(values, cols, maxLenghts)
    return [][][]byte{}, err
}



func processSelectQuery (
				db * types.Database_t,
				sourceTable string, 
				selectedColumns []string,
				joinTables types.Join_t, 
				conditions []types.CompareObj,
				limit uint64) (values [][][]byte, columns []types.Column_t, maxLenghts []int, err error){

    fmt.Println("\n\n\nhere:", sourceTable)
    tbIndex, err := joins.GetTableIndex(db, sourceTable)
    if err != nil {
        return [][][]byte{}, nil, []int{}, err
    }

    currentTb := db.Tables[tbIndex]
		colIndices, err := getColumnIndeces(currentTb, selectedColumns)
    if err != nil {
        return [][][]byte{}, nil, []int{}, err
    }

    if len(joinTables) > 0 {
				fmt.Println("We have a join :)", joinTables)
				// fill columns
				for _, i_col := range colIndices {
						columns = append(columns, currentTb.Columns[i_col])
				}
				// perhaps this should not return here and should be filtered before
				// alternatively, they can be filtered in JOIN (but this might be a little too much)
				// Yet this would enable filtering on every JOIN (which would be very cool)
				return joins.JOIN(db, uint(tbIndex), selectedColumns, joinTables)
    }

    if len(conditions) == 1 {
        fmt.Println("checking if this col is indexed:", conditions[0].ColName)
				fmt.Println(conditions[0])
        if isIndexed, iCol, err := types.IsColIndexed(currentTb, conditions[0].ColName); err != nil {
            return [][][]byte{}, nil, []int{}, err
        } else if isIndexed {
						fmt.Println(currentTb.Columns[iCol])
						fmt.Println(currentTb.Indeces[iCol])
						root := btree.UnsafePAnyToPNode_t(currentTb.Indeces[iCol].Root) 
						btree.Traverse(root, root)
						entry, err := btree.SearchKey(root, root, conditions[0].Value, currentTb.Columns[iCol].Type)
						if err != nil {
								return [][][]byte{}, nil, []int{}, err
						}
						fmt.Println(entry)
						val, _, err := entries.ReadEntryFromFile(currentTb, int(entry.Value)+int(currentTb.StartEntries))
						if err != nil {
								return [][][]byte{}, nil, []int{}, err
						}
						newCols := search.FilterColumns(currentTb.Columns, colIndices)
						maxLenghts = types.GetMaxLengthFromBytes(val, newCols)
            return [][][]byte{val}, columns, maxLenghts, nil
				}
        vals, maxLenghts, err := search.FindEntryWhereCondition(currentTb, colIndices, uint64(limit), conditions...)
        if err != nil {
            return [][][]byte{}, nil, []int{}, err
        }
        return vals, columns, maxLenghts, nil
    }

    if len(conditions) > 1 {
        return [][][]byte{}, nil, []int{}, errors.New("Multiple conditions is not implemented yet")
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
        return [][][]byte{}, nil, []int{}, err
    }

    return values, columns, maxLenghts, nil
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



func SELECT_ALL (table *types.Table_t) (values [][][]byte, maxLenghts []int, err error){
		colIndices := []int{0}
		for i_col := range table.Columns {
				colIndices = append(colIndices, i_col)
		}
    values, maxLenghts, err = search.IterateOverEntriesInFile(table, colIndices, 100)
    if err != nil {
        return [][][]byte{}, []int{}, err
    }
    return values, maxLenghts, nil
		
}



