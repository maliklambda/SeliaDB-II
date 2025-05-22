package entries

import (
"encoding/binary"
"errors"
"fmt"
"os"

"github.com/MalikL2005/SeliaDB-II/btree"
"github.com/MalikL2005/SeliaDB-II/types"
)


func UpdateEntriesWhere (tb *types.Table_t, fh *FileHandler, cmpObj types.CompareObj, colName string, newValue any) error {
    if !ExistsColumnName(tb, cmpObj.ColName){
        return errors.New(fmt.Sprintf("Column %s (compare column) does not exist", cmpObj.ColName))
    }

    if !ExistsColumnName(tb, colName){
        return errors.New(fmt.Sprintf("Column %s (set column) does not exist", colName))
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
    newOffsetsBtree := types.UpdateOffsetList{}
    newOffsetsBtree.UpdateDict = make(map[int]int32)
    var numNewBytes int32
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
            offsetToProperty := getEntryLengthUpToIndex(entry, colIndex)
            fmt.Println("writing", newValue, "at", int32(curOffset) + offsetToProperty)
            numNewBytes, err = updateEntry(fh, tb, &entry, int64(int32(curOffset) + offsetToProperty), colIndex, newValue)
            if err != nil {
                fmt.Println("ERR:",err)
                return err
            }

            fmt.Println("For all entries starting at/after", curOffset+uint16(GetEntryLength(entry))- uint16(newOffsetsBtree.Current), ": Add this many bytes", numNewBytes)
            fmt.Println("But actually add", numNewBytes + newOffsetsBtree.Current)
            if numNewBytes != 0 {
                newOffsetsBtree.UpdateDict[int(curOffset)+GetEntryLength(entry)-int(newOffsetsBtree.Current)] = int32(numNewBytes) + newOffsetsBtree.Current
                newOffsetsBtree.Current += numNewBytes
            }
        }
        curOffset += uint16(GetEntryLength(entry)) + uint16(numNewBytes)
    }
    fmt.Println(newOffsetsBtree)
    if len(newOffsetsBtree.UpdateDict) > 0 {
        fmt.Println("Must update btree entries")
        btree.UpdateBtreeOffsetMap(*fh.Root, &newOffsetsBtree.UpdateDict)
        return nil
    }
    return nil
}




func updateEntry (fh * FileHandler, tb * types.Table_t, entry *[][]byte, offset int64, colIndex int, newValue any) (int32, error) {
    fmt.Println("Updating entry")

    if tb.Columns[colIndex].Type == types.VARCHAR {
        s, ok := newValue.(string)
        if !ok {
            return 0, errors.New("Types do not match. Expexted VARCHAR")
        }
        fmt.Println("Have to deal with varchar length ...")
        fmt.Println(len((*entry)[colIndex]))
        fmt.Println((*entry)[colIndex])
        fmt.Println(string((*entry)[colIndex]))
        fmt.Println(len(s)+1)
        return updateEntryVarchar(fh, entry, offset, colIndex, s)
    }

    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return 0, err
    }
    defer f.Close()

    err = ValidateTypeValue(tb.Columns[colIndex].Type, int(tb.Columns[colIndex].Size), newValue)
    if err != nil {
        return 0, err
    }

    pos, err := f.Seek(offset, 0)
    if err != nil {
        return 0, err
    }

    fmt.Println("current pos:", pos)

    err = binary.Write(f, binary.LittleEndian, newValue)
    if err != nil {
        return 0, err
    }

    fmt.Println("This worked!!!!")

    return 0, nil
}



func getEntryLengthUpToIndex (entry [][]byte, index int) int32 {
    length := 0
    for i := range index {
        length += len(entry[i])
    }
    return int32(length)
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



func updateEntryVarchar (fh * FileHandler, entry *[][]byte, offset int64, colIndex int, newString string) (int32, error) {
    if len((*entry)[colIndex]) < len(newString)+1 {
        fmt.Println("Must allocate", len(newString)+1-len((*entry)[colIndex]), "new bytes on file, at offset", offset+int64(len((*entry)[colIndex])))
        newAllocatedBytes := len(newString)+1 - len((*entry)[colIndex])
        err := types.AllocateInFile(fh.Path, offset+int64(len((*entry)[colIndex])), int64(newAllocatedBytes))
        if err != nil {
            return 0, err
        }
        err = fh.WriteStringToFile(offset, newString)
        if err != nil {
            return 0, err
        }
        return int32(newAllocatedBytes), nil

    } else if len((*entry)[colIndex]) > len(newString)+1 {
        numFreedBytes := len((*entry)[colIndex]) - (len(newString)+1)
        fmt.Println("Must deallocate", numFreedBytes, "many bytes from file, right of offset", offset)
        err := types.DeallocateInFile(fh.Path, offset,  int64(numFreedBytes))
        if err != nil {
            return 0, err
        }
        err = fh.WriteStringToFile(offset, newString)
        if err != nil {
            return 0, err
        }

        return int32(numFreedBytes * -1), nil
    }

    // no bytes allocated or freed on file
    // len((*entry)[colIndex]) == len(newString) +1
    return 0, nil
}




