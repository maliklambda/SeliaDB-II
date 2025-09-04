package joins

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/types"
)


func InnerJoin (db *types.Database_t, 
		left_tb_index uint,
		columns []types.Column_t,
		right_tb_name string,
		join struct{Left string; Right string; How types.JoinType}) (values [][][]byte, cols []types.Column_t, maxLengths []int, err error) {
		left_tb := db.Tables[left_tb_index]
		fmt.Println("Joining", left_tb.Name, "and", right_tb_name, "on", join.Left, "and", join.Right)

		i_right_tb, err := GetTableIndex(db, right_tb_name)
		if err != nil {
				return nil, nil, nil, err
		}
		right_tb := db.Tables[i_right_tb]
		
		right_join_col_name := strip_table_name(join.Right, right_tb_name)
		fmt.Println("right col name:", right_join_col_name)
		is_right_indexed, i_right_join_col, err := types.IsColIndexed(right_tb, right_join_col_name)
		if err != nil {
				return nil, nil, nil, err
		}

		left_join_col_name := strip_table_name(join.Left, left_tb.Name)
		fmt.Println("left col name:", right_join_col_name)
		_, i_left_join_col, err := types.IsColIndexed(left_tb, left_join_col_name)
		if err != nil {
				return nil, nil, nil, err
		}

		// if not the same type, join is not possible
		if right_tb.Columns[i_right_join_col].Type != left_tb.Columns[i_left_join_col].Type {
				return nil, nil, nil, fmt.Errorf("Missmatch of join-types: %s (left) -> %s and %s (right) -> %s", 
						left_tb.Columns[i_left_join_col].Name, left_tb.Columns[i_left_join_col].Type.String(), 
						right_join_col_name, right_tb.Columns[i_right_join_col].Type.String())
		}

		// append right_tb_columns to return_columns
		for _, col := range right_tb.Columns {
				col.Name = right_tb_name + "." + col.Name
				columns = append(columns, col)
		}
		
		if is_right_indexed {
				fmt.Println("Yay, n * log(n)")
				fmt.Println("Looking up index of", right_tb.Columns[i_right_join_col].Name)
				root := btree.UnsafePAnyToPNode_t(right_tb.Indeces[i_right_join_col].Root)
				btree.Traverse(root, root)
		} else {
				fmt.Println("Awwwwww no index for this join: n² :(")
		}
		fmt.Println(columns)
		return nil, nil, nil, fmt.Errorf("Inner join is not fully implemented yet")
		// return values, columns, maxLengths, nil
}



// func InnerJoinIndexed () (values [][][]byte, joinedTb *types.Table_t, maxLengths []int, err error) {
// }


func displayTableAndColumn (table * types.Table_t, colIndex int) string {
    column := table.Columns[colIndex]
    return fmt.Sprintf("%s.%s (%s)", table.Name, column.Name, column.Type.String())
}

func mergeTables (left, right *types.Table_t, joinCols[] struct{
        left int;
        right int;
    }) (mergedTable *types.Table_t, err error) {
    left_cols := make([]types.Column_t, left.NumOfColumns)
    copy(left_cols, left.Columns)
    right_cols := make([]types.Column_t, right.NumOfColumns)
    copy(right_cols, right.Columns)
    changedNames := make([]int, 0)
    for i, col := range joinCols {
        // rename col in right 
        right_cols[col.right].Name = left.Columns[col.left].Name + "_" + right.Columns[col.right].Name
        // delete col from left
        left_cols = slices.Delete(left_cols, col.left, col.left+1)
        fmt.Println(col)
        changedNames = append(changedNames, i)
    }
    // rename colname to table.colname
    for i := range left_cols {
        left_cols[i].Name = left.Name + "." + left_cols[i].Name
    }
    for i := range right_cols {
        if slices.Contains(changedNames, i-1){
            continue
        }
        right_cols[i].Name = right.Name + "." + right_cols[i].Name
    }
    mergedTable = &types.Table_t {
        NumOfColumns: left.NumOfColumns + right.NumOfColumns - uint32(len(joinCols)),
        Name: "tmp_" + left.Name + "_" + right.Name,
        Columns: append(left_cols, right_cols...),
    }
    
    return mergedTable, nil
}



// Like InnerJoinIndexedSingleCol but left has values in memory 
// Right must still be indexed
// -> useful for multiple joins ("JOIN t1 ON ... JOIN t2 ON ...")
func InnerJoinIndexedChained (left, right * types.Table_t, leftValues [][][]byte, cols []types.Column_t, leftCol, rightCol string) (values [][][]byte, joinedTb *types.Table_t, columns []types.Column_t, maxLengths []int, err error) {
    fmt.Println("Joining ", left.Name, " and ", right.Name)
    leftIndex, err := entries.StringToColumnIndex(left, leftCol)
    if err != nil {
        return [][][]byte{}, nil, []types.Column_t{}, []int{}, err
    }

    rightIndex, err := entries.StringToColumnIndex(right, rightCol)
    if err != nil {
        return [][][]byte{}, nil, []types.Column_t{}, []int{}, err
    }
    if !right.IsColIndexed(uint32(rightIndex)){
        return [][][]byte{}, nil, []types.Column_t{}, []int{}, errors.New(fmt.Sprint("Cannot join on non-indexed column ", displayTableAndColumn(right, rightIndex), "(yet)"))
    }

    if left.Columns[leftIndex].Type != right.Columns[rightIndex].Type {
        return [][][]byte{}, nil, []types.Column_t{}, []int{}, errors.New(fmt.Sprint(
            "Cannot join tables ", displayTableAndColumn(left, leftIndex), " and ",
            displayTableAndColumn(right, rightIndex),
            " -> missmatched types"))
        }

    rightIndicesIndex, err := right.FindIndex(uint32(rightIndex))
    if err != nil {
        return [][][]byte{}, nil, []types.Column_t{}, []int{}, err
    }
    rt := btree.UnsafePAnyToPNode_t(right.Indeces[rightIndicesIndex].Root)
    if rt == nil || rt.Entries == nil {
        return [][][]byte{}, nil, []types.Column_t{}, []int{}, fmt.Errorf("Root of %s.%s is Nil.", right.Name, right.Columns[rightIndex].Name)
    }

    newTb, err := mergeTables(left, right, []struct {left int; right int}{
        {left: leftIndex, right: rightIndex},
    })
    if err != nil {
        return [][][]byte{}, nil, []types.Column_t{}, []int{}, err
    }
    maxLengths = make([]int, len(newTb.Columns))

    currentPos := left.StartEntries
    values = [][][]byte{}
    fmt.Println(left)
    fmt.Println(right)
    fmt.Print("\n\n\n\n\n\n")
    for _, buffer := range leftValues {
        fmt.Println("Reading entry (outer) at", currentPos)
        currentPos += uint16(entries.GetEntryLength(buffer))

        // find current value from left in right
        val, err := types.ByteSliceToValue(buffer[leftIndex], left.Columns[leftIndex].Type)
        if err != nil {
            return [][][]byte{}, nil, []types.Column_t{}, []int{}, err
        }
        fmt.Println("finding ", val, " in right-join-column")
        entry, err := btree.SearchKey(rt, rt, val, right.Columns[rightIndex].Type)
        if err != nil {
            continue
        }
        
        // check here for filter on right column -> continue if not met


        // remove join column from left
        rightValues, _, err := entries.ReadEntryFromFile(right, int(entry.Value))
        new_vals := append(slices.Delete(buffer, leftIndex, leftIndex +1), rightValues...)
        // update longest display value
        maxLengths = types.UpdateLongestDisplay(maxLengths, new_vals, newTb.Columns)
        fmt.Println(maxLengths)
        values = append(values, new_vals)
    }
    fmt.Print("\n\n\n\n\n\n")
    btree.Traverse(rt, rt)
    fmt.Println("Max lengths:", maxLengths)
    return values, newTb, columns, maxLengths, nil
}



// remove tablename from right_column
// "tb_name.col_name" becomes "col_name"
func strip_table_name (colName, tableName string) string {
		if strings.HasPrefix(colName, tableName + ".") {
				return colName[len(tableName) + 1:]
		}
		return colName
}



