package entries

import (
	"encoding/binary"
	"errors"
	"fmt"
    "os"

	"github.com/MalikL2005/SeliaDB-II/types"
)

func UpdateEntriesWhere (tb *types.Table_t, fh *FileHandler, cmpObj types.CompareObj, colName string, newValue any) error {
    if !ExistsColumnName(tb, cmpObj.ColName){
        return errors.New(fmt.Sprintf("Column %s (compare column) does not exist", cmpObj.ColName))
    }

    if !ExistsColumnName(tb, colName){
        return errors.New(fmt.Sprintf("Column %s does not exist", colName))
    }
    
    colIndex, err := StringToColumnIndex(tb, colName)
    if err != nil {
        return err
    }

    err = iterateOverEntriesUpdate(fh, tb, cmpObj, colIndex, newValue)
    if err != nil {
        return err
    }
    
    return nil
    
}





func iterateOverEntriesUpdate (fh *FileHandler, tb *types.Table_t, cmp types.CompareObj, colIndex int, newValue any) error {
    fmt.Println(cmp.ColName, cmp.Value)
    cmpColIndex, err := StringToColumnIndex(tb, cmp.ColName)
    if err != nil {
        return err
    }

    curOffset := tb.StartEntries
    for range tb.Entries.NumOfEntries {
        entry, err := ReadEntryFromFile(tb, int(curOffset), fh)
        if err != nil {
            return err
        }
        fmt.Println("Comparing", entry, "and", cmp.Value)
        // check if entry matches condition
        compareResult, err := types.CompareValues(tb.Columns[cmpColIndex].Type, entry[cmpColIndex], cmp.Value)
        if err != nil {
            return err
        }
        fmt.Println("Return result:", compareResult)
        if types.CompareValuesWithOperator(compareResult, cmp.CmpOperator) {
            fmt.Println("Condition matches!!!")
            fmt.Println("current", curOffset)
            offsetToProperty := getEntryLengthUpToIndex(tb, entry, colIndex)
            fmt.Println("writing", newValue, "at", int64(int(curOffset) + offsetToProperty))
            err = updateEntry(fh, tb, int64(int(curOffset) + offsetToProperty), colIndex, newValue)
            if err != nil {
                fmt.Println("ERR:",err)
                return err
            }
        }
        curOffset += uint16(GetEntryLength(entry))
    }
    return nil
}




func updateEntry (fh * FileHandler, tb * types.Table_t, offset int64, colIndex int, newValue any) error {
    fmt.Println("Updating entry")

    if tb.Columns[colIndex].Type == types.VARCHAR {
        fmt.Println("Have to deal with varchar length ...")
        return errors.New("Not implemented yet.")
    }

    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    err = ValidateTypeValue(tb.Columns[colIndex].Type, int(tb.Columns[colIndex].Size), newValue)
    if err != nil {
        return err
    }

    pos, err := f.Seek(offset, 0)
    if err != nil {
        return err
    }

    fmt.Println("current pos:", pos)

    err = binary.Write(f, binary.LittleEndian, newValue)
    if err != nil {
        return err
    }

    fmt.Println("This worked!!!!")

    return nil
}



func getEntryLengthUpToIndex (tb *types.Table_t, entry [][]byte, index int) int {
    length := 0
    for i := 0; i<index; i++ {
        length += len(entry[i])
    }
    return length
}




func StringToColumnIndex (tb *types.Table_t, colName string) (int, error){
    for i := range tb.Columns {
        if tb.Columns[i].Name == colName {
            return i, nil
        }
    }
    return 0, errors.New(fmt.Sprintf("Column %s does not exist", colName))
}



func ValidateTypeValue (colType types.Type_t, colSize int, defaultValue any) error {
    switch (colType){
    case types.INT32:
        _, ok := defaultValue.(int32)
        if !ok {
            return errors.New("Expected type to be int32. defaultvalue does not match")
        }
    case types.FLOAT32:
        _, ok := defaultValue.(float32)
        if !ok {
            return errors.New("Expected type to be float32. defaultvalue does not match")
        }
    case types.BOOL:
        _, ok := defaultValue.(bool)
        if !ok {
            return errors.New("Expected type to be bool. defaultvalue does not match")
        }
    case types.VARCHAR:
        s, ok := defaultValue.(string)
        if !ok {
            return errors.New("Expected type to be varchar. defaultvalue does not match")
        }
        if len(s) > colSize {
            return errors.New(fmt.Sprintf("Expected a varchar length of max %d but defaultvalue has a length of %d", colSize, len(s)))
        }
    }
    return nil
}



func ExistsColumnName (tb *types.Table_t, colName string) bool {
    for _, column := range tb.Columns {
        if column.Name == colName {
            return true
        }
    }
    return false
}



