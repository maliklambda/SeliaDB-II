package search

import (
	"fmt"

	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/types"
)

// Depracated! Use IterateOverEntriesInFile (better because entries don't have to be loaded into memory)
func IterateOverEntries(tb *types.Table_t){
    fmt.Println("Iterating over entries!!!!!!")
    for cur := range tb.Entries.NumOfEntries {
        fmt.Println("Entry number", cur+1)
        _, err := entries.ReadEntryIndex(tb, int(cur))
        if err != nil {
            fmt.Println(err)
            break
        }
    }
    fmt.Println("Here")
}



func IterateOverEntriesInFile (tb *types.Table_t, selectedColumnsIndeces []int, limit uint64) ([][][]byte, []int, error) {
    fmt.Println("Iterating over entries on file!!!!!!")
    maxLengths := make([]int, len(tb.Columns))
    cols := FilterColumns(tb.Columns, selectedColumnsIndeces)
    var currentPos uint32 = uint32(tb.StartEntries)
    values := [][][]byte{}
    for {
        if uint64(len(values)) >= limit {
            break
        }
        fmt.Println("Reading entry at", currentPos)
        buffer, pNextEntry, err := entries.ReadEntryFromFile(tb, int(currentPos))
        if err != nil {
            break
        }
        if len(selectedColumnsIndeces) > 0 {
            buffer = filterBufferByColumnIndices(buffer, selectedColumnsIndeces)
        }
        fmt.Println("Next entry:", pNextEntry)
        values = append(values, buffer)
        maxLengths = types.UpdateLongestDisplay(maxLengths, buffer, cols)
        currentPos = uint32(pNextEntry)
    }
    fmt.Println("Here")
    fmt.Println(values)
    return values, maxLengths, nil
}



func FindEntryByKey (tb *types.Table_t, colName string, value any) ([][]byte, error) {
    fmt.Println(colName, value)
    index, err := entries.StringToColumnIndex(tb, colName)
    if err != nil {
        return [][]byte{}, err
    }
    fmt.Println(tb.Columns[index])
    for cur := range tb.Entries.NumOfEntries {
        entry, err := entries.ReadEntryIndex(tb, int(cur))
        if err != nil {
            return [][]byte{}, err
        }
        fmt.Println(entry[index])
        fmt.Println(tb.Columns[index].Type)
        i, err := types.CompareValues(tb.Columns[index].Type, entry[index], value)
        if err != nil {
            return [][]byte{}, err
        }
        // values are equal
        if i == 0 {
            fmt.Println("Found right entry")
            return entry, nil
        }
    }
    fmt.Println("Not found")
    return [][]byte{}, nil
}




func FindEntryWhereCondition (tb *types.Table_t, limit uint16, cmpObjs ... types.CompareObj) ([][][]byte, []int, error){
    indices := make([]int, len(cmpObjs))
    for i, cmp := range cmpObjs {
        index, err := entries.StringToColumnIndex(tb, cmp.ColName)
        if err != nil {
            return [][][]byte{}, []int{}, err
        }
        fmt.Println(tb.Columns[index])
        indices[i] = index
    }

    if limit < 1 {
        limit = 1
    }
    
    maxLengths := make([]int, 0)
    returnValues := make([][][]byte, 0)
    cur := tb.StartEntries
    for range tb.Entries.NumOfEntries {
        entry, _, err := entries.ReadEntryFromFile(tb, int(cur))
        if err != nil {
            return [][][]byte{}, []int{}, err
        }
        cur += uint16(entries.GetEntryLength(entry))
        for i, cmp := range cmpObjs {
            fmt.Println("Comparing", entry[indices[i]], "and", cmp.Value)
            // check if entry matches condition
            compareResult, err := types.CompareValues(tb.Columns[indices[i]].Type, entry[indices[i]], cmp.Value)
            if err != nil {
                return [][][]byte{}, []int{}, err
            }
            if !types.CompareValuesWithOperator(compareResult, cmp.CmpOperator) {
                break
            }
            if i == len(cmpObjs) -1 {
                returnValues = append(returnValues, entry)
            }
            maxLengths = types.UpdateLongestDisplay(maxLengths, entry, tb.Columns)
        }
        // if limit is exceeded, break out
        if len(returnValues) >= int(limit) {
            return returnValues, []int{}, nil
        }
    }
    return returnValues, maxLengths, nil
}



func filterBufferByColumnIndices(buffer [][]byte, selectedColumnsIndeces[]int) [][]byte {
    newBuf := make([][]byte, 0)
    for _, index := range selectedColumnsIndeces {
        newBuf = append(newBuf, buffer[index])
    }
    return newBuf
}



func FilterColumns (cols []types.Column_t, selectedColumnsIndeces[]int) []types.Column_t {
    fmt.Println(cols)
    fmt.Println(selectedColumnsIndeces)
    newCols := make([]types.Column_t, 0)
    for _, index := range selectedColumnsIndeces {
        newCols = append(newCols, cols[index])
    }
    return newCols
}
