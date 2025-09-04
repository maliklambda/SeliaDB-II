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



func JOIN (db *types.Database_t, i_start_tb uint, selectedCols []string, joinObj types.Join_t) (values [][][]byte, columns []types.Column_t, maxLengths []int, err error){
		start_tb := db.Tables[i_start_tb]
		for _, col := range start_tb.Columns {
				col.Name = start_tb.Name + "." + col.Name
				columns = append(columns, col)
		}
		values, maxLengths, err = SELECT_ALL(start_tb)
		if err != nil {
				return [][][]byte{}, []types.Column_t{}, []int{}, err
		}

		// perhabs this is an issue with the ordering of this, since hash-maps do not garantee to keep order
		for right_tb_name, join := range joinObj {
				if err != nil {
						return [][][]byte{}, []types.Column_t{}, []int{}, err
				}
				switch join.How {
						case types.INNER:
								values, columns, maxLengths, err = InnerJoin(db, i_start_tb, columns, right_tb_name, join)
								if err != nil {
										return [][][]byte{}, []types.Column_t{}, []int{}, fmt.Errorf("Could not join columns: %s", err)
								}
								fmt.Println("successful INNER JOIN with ", right_tb_name)
						case types.LEFT:
						case types.RIGHT:
						case types.OUTER:
						case types.LEFT_OUTER:
						case types.RIGHT_OUTER:
				}
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




func SELECT_ALL (table *types.Table_t) (values [][][]byte, maxLenghts []int, err error){
		colIndices := []int{0}
		for i_col := range table.Columns {
				colIndices = append(colIndices, i_col)
		}
    values, maxLenghts, err = search.IterateOverEntriesInFile(table, colIndices, 0)
    if err != nil {
        return [][][]byte{}, []int{}, err
    }
    return values, maxLenghts, nil
		
}


