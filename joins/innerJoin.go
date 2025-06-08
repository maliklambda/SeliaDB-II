package joins

import (
	"errors"
	"fmt"
	"slices"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/types"
)

type joinCompareObj struct {
    cmpObj types.CompareObj
    cmpCol struct {
        name string
        index uint
    }
}

func InnerJoinIndexedSingleCol (left, right * types.Table_t, leftCol, rightCol string) ([][][]byte, *types.Table_t, error) {
    fmt.Println("Joining ", left.Name, " and ", right.Name)
    leftIndex, err := entries.StringToColumnIndex(left, leftCol)
    if err != nil {
        return [][][]byte{}, nil, err
    }
    if !left.IsColIndexed(uint32(leftIndex)){
        return [][][]byte{}, nil, errors.New(fmt.Sprint("Cannot join on non-indexed column ", displayTableAndColumn(left, leftIndex), " (yet)"))
    }

    rightIndex, err := entries.StringToColumnIndex(right, rightCol)
    if err != nil {
        return [][][]byte{}, nil, err
    }
    if !right.IsColIndexed(uint32(rightIndex)){
        return [][][]byte{}, nil,errors.New(fmt.Sprint("Cannot join on non-indexed column ", displayTableAndColumn(right, rightIndex), "(yet)"))
    }

    if left.Columns[leftIndex].Type != right.Columns[rightIndex].Type {
        return [][][]byte{}, nil, errors.New(fmt.Sprint(
            "Cannot join tables ", displayTableAndColumn(left, leftIndex), " and ",
            displayTableAndColumn(right, rightIndex),
            " -> missmatched types"))
        }

    rightIndicesIndex, err := right.FindIndex(uint32(rightIndex))
    if err != nil {
        return [][][]byte{}, nil, err
    }

    newTb, err := mergeTables(left, right, []struct {left int; right int}{
        {left: leftIndex, right: rightIndex},
    })
    if err != nil {
        return [][][]byte{}, nil, err
    }

    currentPos := left.StartEntries
    values := [][][]byte{}
    fmt.Println(left)
    fmt.Println(right)
    fmt.Print("\n\n\n\n\n\n")
    for {
        fmt.Println("Reading entry (outer) at", currentPos)
        buffer, err := entries.ReadEntryFromFile(left, int(currentPos))
        if err != nil {
            fmt.Println(err)
            break
        }
        currentPos += uint16(entries.GetEntryLength(buffer))

        // find current value from left in right
        val, err := types.ByteSliceToValue(buffer[leftIndex], left.Columns[leftIndex].Type)
        if err != nil {
            return [][][]byte{}, nil, err
        }
        fmt.Println("finding ", val, " in right-join-column")
        rt := btree.UnsafePAnyToPNode_t(right.Indeces[rightIndicesIndex].Root)
        entry, err := btree.SearchKey(rt, rt, val, right.Columns[rightIndex].Type)
        if err != nil {
            continue
        }
        // remove join column from left
        rightValues, err := entries.ReadEntryFromFile(right, int(entry.Value))
        new_vals := append(slices.Delete(buffer, leftIndex, leftIndex +1), rightValues...)
        values = append(values, new_vals)
    }
    fmt.Print("\n\n\n\n\n\n")
    fmt.Println(right.Indeces)
    fmt.Println(rightIndicesIndex)
    rt := btree.UnsafePAnyToPNode_t(right.Indeces[rightIndicesIndex].Root)
    btree.Traverse(rt, rt)
    return values, newTb, nil
}


func displayTableAndColumn (table * types.Table_t, colIndex int) string {
    column := table.Columns[colIndex]
    return fmt.Sprintf("%s.%s (%s)", table.Name, column.Name, column.Type.String())
}

func mergeTables (left, right *types.Table_t, joinCols[] struct{
        left int;
        right int;
    }) (*types.Table_t, error) {
    left_cols := make([]types.Column_t, left.NumOfColumns)
    copy(left_cols, left.Columns)
    right_cols := make([]types.Column_t, right.NumOfColumns)
    copy(right_cols, right.Columns)
    for _, col := range joinCols {
        // rename col in right 
        right_cols[col.right].Name = left.Columns[col.left].Name + "_" + right.Columns[col.right].Name
        // delete col from left
        left_cols = slices.Delete(left_cols, col.left, col.left+1)
        fmt.Println(col)
    }
    newTb := types.Table_t {
        NumOfColumns: left.NumOfColumns + right.NumOfColumns - uint32(len(joinCols)),
        Name: "tmp_" + left.Name + "_" + right.Name, 
        Columns: append(left_cols, right_cols...),
    }
    
    return &newTb, nil
}



