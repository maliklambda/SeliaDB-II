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
		prev_max_lengths types.MaxLengths_t,
		join struct{Left string; Right string; How types.JoinType}) (values [][][]byte, cols []types.Column_t, maxLengths types.MaxLengths_t, err error) {
		left_tb := db.Tables[left_tb_index]
		fmt.Println("Joining", left_tb.Name, "and", right_tb_name, "on", join.Left, "and", join.Right)

		i_right_tb, err := GetTableIndex(db, right_tb_name)
		if err != nil {
				return nil, nil, nil, err
		}
		right_tb := db.Tables[i_right_tb]
		
		right_join_col_name := strip_table_name(join.Right, right_tb_name)
		fmt.Println("right col name:", right_join_col_name, " - right tb name:", right_tb_name)
		fmt.Println(right_tb.Indeces)
		fmt.Println(right_tb.Columns)
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
		
		// fill values
		values, maxLengths, err = SELECT_ALL(left_tb)
		if err != nil {
				return nil, nil, nil, fmt.Errorf("Failed select all: %s", err)
		}
	
		
		fmt.Println(is_right_indexed)		
		if is_right_indexed {
				fmt.Println("Yay, n * log(n)")
				fmt.Println("Looking up index of", right_tb.Columns[i_right_join_col].Name)
				root := btree.UnsafePAnyToPNode_t(right_tb.Indeces[i_right_join_col].Root)
				btree.Traverse(root, root)
				values, maxLengths, err = InnerJoinIndexed(values, columns, 0, root, right_tb, uint(i_right_join_col), prev_max_lengths)
				if err != nil {
						return nil, nil, nil, fmt.Errorf("InnerJoinIndexed failed: %s", err)
				}
		} else {
				fmt.Println("Awwwwww no index for this join: n² :(")
				return nil, nil, nil, fmt.Errorf("Inner join for non indexed column is not fully implemented yet")
		}
		fmt.Println(columns)
		return values, columns, maxLengths, nil
}



func InnerJoinIndexed (left_values [][][]byte, left_cols []types.Column_t, left_join_col_index uint, 
		root_right *btree.Node_t,
		right_tb *types.Table_t, right_join_col_index uint, maxLengths_left types.MaxLengths_t) (values [][][]byte, maxLengths types.MaxLengths_t, err error) {
		tp := left_cols[left_join_col_index].Type
		var right_val [][]byte

		parse_type := tp.GetTypeParser()
		right_max_lengths := make([]int, len(right_tb.Columns))
		fmt.Println("left vals:", left_values)
		for _, left_val := range left_values {
				joined_left_val := left_val[left_join_col_index]
				fmt.Println("joined left val", joined_left_val)
				lookup_val, err := parse_type(joined_left_val)
				if err != nil {
						return nil, nil, fmt.Errorf("Failed to parse type (for %s): %s", joined_left_val, err)
				}
				fmt.Println("Looking for index of", lookup_val)
				right_index, err := btree.SearchKey(root_right, root_right, lookup_val, tp)
				if err != nil {
						return nil, nil, fmt.Errorf("Failed to look up key %s: %s", lookup_val, err)
				}
				
				if right_index != nil {
						fmt.Println("Reading entry @", right_index)
						right_val, _, err = entries.ReadEntryFromFile(right_tb, int(right_index.Value + uint32(right_tb.StartEntries)))
						if err != nil {
								return nil, nil, fmt.Errorf("Failed to read entry: %s", err)
						}
						right_max_lengths = types.UpdateLongestDisplay(right_max_lengths, right_val, right_tb.Columns)
						
						values = append(values, append(left_val, right_val...))
				}
		}
		// extend maxLengths with right_cols
		maxLengths = maxLengths_left
		for _, v := range right_max_lengths {
				maxLengths = append(maxLengths, v)
		}
		fmt.Println(values)
		return values, maxLengths, nil
}


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



