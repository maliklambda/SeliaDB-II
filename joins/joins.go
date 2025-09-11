package joins

import (
	"fmt"

	"github.com/MalikL2005/SeliaDB-II/types"
	"github.com/MalikL2005/SeliaDB-II/search"
)


type joinCompareObj struct {
    cmpObj types.CompareObj
    cmpCol struct {
        name string
        index uint
    }
}



// for chained joins: right column must be the latter column in query
func JOIN (db *types.Database_t, i_start_tb uint, selectedCols []string, joinObj types.Join_t) (values types.Values_t, columns []types.Column_t, maxLengths types.MaxLengths_t, err error){
		start_tb := db.Tables[i_start_tb]
		tables := &[]types.Table_t{*start_tb}
		for _, col := range start_tb.Columns {
				col.Name = start_tb.Name + "." + col.Name
				columns = append(columns, col)
		}
		values, maxLengths, err = SELECT_ALL(start_tb)
		if err != nil {
				return types.Values_t{}, []types.Column_t{}, []int{}, err
		}
		fmt.Println("Original values:", values)

		// perhabs this is an issue with the ordering of joinObj, since hash-maps do not garantee to keep order
		for right_tb_name, join := range joinObj {
				if len(values) == 0 {
						return nil, nil, nil, fmt.Errorf("Empty set")
				}
				fmt.Println(tables)
				switch join.How {
						case types.INNER:
								values, tables, columns, maxLengths, err = InnerJoin(db, tables, columns, values, right_tb_name, maxLengths, join)
								if err != nil {
										return types.Values_t{}, []types.Column_t{}, []int{}, fmt.Errorf("Could not join columns: %s", err)
								}
								fmt.Printf("successful INNER JOIN: with %s\n", right_tb_name)
						case types.LEFT:
						case types.RIGHT:
						case types.OUTER:
						case types.LEFT_OUTER:
						case types.RIGHT_OUTER:
				}
		}
		fmt.Println(values)
		fmt.Println(columns)
		fmt.Println(maxLengths)
		if len(maxLengths) == 0 {
				return nil, nil, nil, fmt.Errorf("Empty maxLengths returned from join")
		}
		return values, columns, maxLengths, nil
}



func GetTableIndex (db * types.Database_t, s string) (int, error) {
    for i, tb := range db.Tables {
        if tb.Name == s {
            return i, nil
        }
    }
    return -1, fmt.Errorf("Table %s does not exist in %s", s, db.Name)
}




func SELECT_ALL (table *types.Table_t) (values types.Values_t, maxLenghts types.MaxLengths_t, err error){
		fmt.Println("every colllll")
		colIndices := []int{}
		for i := range table.Columns {
				colIndices = append(colIndices, i)
		}
    values, maxLenghts, err = search.IterateOverEntriesInFile(table, colIndices, 10000)
    if err != nil {
        return types.Values_t{}, []int{}, err
    }
    return values, maxLenghts, nil
}


