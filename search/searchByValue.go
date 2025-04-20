package search

import (
	"errors"
	"fmt"

	"github.com/MalikL2005/Go_DB/entries"
	"github.com/MalikL2005/Go_DB/types"
)

func IterateOverEntries(tb types.Table_t){
    fmt.Println("Iterating over entries!!!!!!")
    for cur := range tb.Entries.NumOfEntries {
        fmt.Println("Entry number", cur+1)
        _, err := entries.ReadEntry (tb, int(cur))
        if err != nil {
            fmt.Println(err)
            break
        }
    }
    fmt.Println("Here")
}


func FindEntryByKey (tb types.Table_t, colName string, value any) ([][]byte, error) {
    fmt.Println(colName, value)
    index, err := StringToColumnIndex(tb, colName)
    if err != nil {
        return [][]byte{}, err
    }
    fmt.Println(tb.Columns[index])
    for cur := range tb.Entries.NumOfEntries {
        entry, err := entries.ReadEntry(tb, int(cur))
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



func StringToColumnIndex (tb types.Table_t, colName string) (int, error){
    for i := range tb.Columns {
        if tb.Columns[i].Name == colName {
            return i, nil
        }
    }
    return 0, errors.New(fmt.Sprintf("Column %s does not exist", colName))
}
