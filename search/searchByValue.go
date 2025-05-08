package search

import (
	"errors"
	"fmt"

	"github.com/MalikL2005/Go_DB/entries"
	"github.com/MalikL2005/Go_DB/types"
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



func IterateOverEntriesInFile (fh *entries.FileHandler, tb *types.Table_t) error {
    fmt.Println("Iterating over entries on file!!!!!!")
    currentPos := tb.StartEntries
    values := [][][]byte{}
    for range tb.Entries.NumOfEntries {
        fmt.Println("Reading entry at", currentPos)
        buffer, err := entries.ReadEntryFromFile(tb, int(currentPos), fh)
        if err != nil {
            return err
        }
        fmt.Println("Buffer len:",entries.GetEntryLength(buffer))
        values = append(values, buffer)
        currentPos += uint16(entries.GetEntryLength(buffer))
    }
    fmt.Println("Here")
    fmt.Println(values)
    return nil
}



func FindEntryByKey (tb *types.Table_t, colName string, value any) ([][]byte, error) {
    fmt.Println(colName, value)
    index, err := StringToColumnIndex(tb, colName)
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



func StringToColumnIndex (tb *types.Table_t, colName string) (int, error){
    for i := range tb.Columns {
        if tb.Columns[i].Name == colName {
            return i, nil
        }
    }
    return 0, errors.New(fmt.Sprintf("Column %s does not exist", colName))
}



func FindEntryWhereCondition (fh *entries.FileHandler, tb *types.Table_t, colName string, value any, cmpOperator types.CompareOperator, limit uint16) ([][][]byte, error){
    fmt.Println(colName, value)
    index, err := StringToColumnIndex(tb, colName)
    if err != nil {
        return [][][]byte{}, err
    }
    fmt.Println(tb.Columns[index])
    if limit < 1 {
        limit = 1
    }
    
    returnValues := make([][][]byte, 0)
    cur := tb.StartEntries
    for range tb.Entries.NumOfEntries {
        entry, err := entries.ReadEntryFromFile(tb, int(cur), fh)
        if err != nil {
            return [][][]byte{}, err
        }
        cur += uint16(entries.GetEntryLength(entry))
        fmt.Println(entry[index])
        fmt.Println(tb.Columns[index].Type)
        // check if entry matches condition
        compareResult, err := types.CompareValues(tb.Columns[index].Type, entry[index], value)
        if err != nil {
            return [][][]byte{}, err
        }
        if types.CompareValuesWithOperator(compareResult, cmpOperator) {
            returnValues = append(returnValues, entry)
        }
        // if limit is exceeded, break out
        if len(returnValues) >= int(limit) {
            return returnValues, nil
        }
    }
    return returnValues, nil
}



