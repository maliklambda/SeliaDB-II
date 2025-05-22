package entries

import (
	"fmt"
	"io"
	"os"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/types"
    "errors"
)

func DeleteAllEntries (tb *types.Table_t, fh *FileHandler) error {
    if tb.Entries != nil && tb.Entries.NumOfEntries == 0 {
        return nil
    }
    f, err := os.OpenFile(fh.Path, os.O_RDWR, 0644)
    if err != nil {
        return err
    }
    defer f.Close()
    end, err := f.Seek(0, 2)
    if err != nil {
        return err
    }
    err = DeleteBytesFromTo(fh, int64(tb.StartEntries), end)
    if err != nil {
        return err
    }
    UpdateOffsetLastEntry(fh, 0)
    return nil
}



func DeleteBytesFromTo (fh *FileHandler, from, to int64) error {
    fmt.Println("Deleting from", from, "to", to)
    f, err := os.Open(fh.Path)
    if err != nil {
        return err
    }
    defer f.Close()

    tmp, err := os.CreateTemp("", "tmp-" + fh.Path)
    if err != nil {
        return err
    }
    defer tmp.Close()

    _, err = io.CopyN(tmp, f, from)
    if err != nil {
        return err
    }

    _, err = f.Seek(to, 0)
    if err != nil {
        return err
    }

    _, err = io.Copy(tmp, f)
    if err != nil {
        return err
    }

    fmt.Println(tmp)
    tmp.Close()
    f.Close()

    err = os.Rename(tmp.Name(), fh.Path)
    if err != nil {
        return err
    }

    return nil
}


func DeleteEntryByPK (tb *types.Table_t, fh *FileHandler, pk uint32) error {
    entry := btree.SearchKey(fh.Root, *fh.Root, pk)
    if entry == nil {
        return errors.New("PK was not found")
    }

    values, err := ReadEntryFromFile(tb, int(entry.Value), fh)
    if err != nil {
        return err
    }
    
    length := GetEntryLength(values)
    if length == 0 {
        return errors.New("length of entry returned 0")
    }

    err = DeleteBytesFromTo(fh, int64(entry.Value), int64(int(entry.Value)+length))
    if err != nil {
        return err
    }

    fmt.Println(entry)
    fmt.Println("removing", length, "after offset", entry.Value)
    newBtreeOffset := types.UpdateOffsetList{}
    newBtreeOffset.UpdateDict = make(map[int]int32)
    newBtreeOffset.UpdateDict[int(entry.Value)] = int32(length)* -1
    fmt.Println(newBtreeOffset)



    // Remove pk from btree
    // btree.Delete(fh.Root, *fh.Root, pk)

    // Update Btree.values where value > entry.Value


    return nil
}





func (fh * FileHandler) DeleteEntriesWhere (tb *types.Table_t, cmpObj types.CompareObj) error {
    if !ExistsColumnName(tb, cmpObj.ColName){
        return errors.New(fmt.Sprintf("Column %s (compare column) does not exist", cmpObj.ColName))
    }

    // colIndex, err := StringToColumnIndex(tb, colName)
    // if err != nil {
    //     return err
    // }

    iterateOverEntriesDelete (fh, tb, cmpObj)
    return nil
}



func iterateOverEntriesDelete (fh *FileHandler, tb *types.Table_t, cmp types.CompareObj) error {
    fmt.Println(cmp.ColName, cmp.Value)
    cmpColIndex, err := StringToColumnIndex(tb, cmp.ColName)
    if err != nil {
        return err
    }

    curOffset := tb.StartEntries
    // newOffsetsBtree := make([]types.UpdateOffsetDict, 0)
    newOffsetsBtree := types.UpdateOffsetList{}
    newOffsetsBtree.UpdateDict = make(map[int]int32)
    for range tb.Entries.NumOfEntries {
        entry, err := ReadEntryFromFile(tb, int(curOffset), fh)
        if err != nil {
            return err
        }
        fmt.Println("Comparing", string(entry[cmpColIndex]), "and", cmp.Value)
        // check if entry matches condition
        compareResult, err := types.CompareValues(tb.Columns[cmpColIndex].Type, entry[cmpColIndex], cmp.Value)
        if err != nil {
            return err
        }
        fmt.Println("Return result:", compareResult)
        fmt.Println(cmp.CmpOperator)
        if types.CompareValuesWithOperator(compareResult, cmp.CmpOperator) {
            fmt.Println("Condition matches!!!")
            fmt.Println("deleting", int32(GetEntryLength(entry))*(-1), "bytes at", curOffset)
            newOffsetsBtree.UpdateDict[int(curOffset)] = int32(GetEntryLength(entry))* (-1)
            newOffsetsBtree.Current -= int32(GetEntryLength(entry))
            err = DeleteBytesFromTo(fh, int64(curOffset), int64(curOffset+uint16(GetEntryLength(entry))))
            if err != nil {
                return err
            }
        } else {
            curOffset += uint16(GetEntryLength(entry))
        }
    }
    fmt.Println(newOffsetsBtree)
    if len(newOffsetsBtree.UpdateDict) > 0 {
        fmt.Println("Must update btree entries")
        btree.UpdateBtreeOffsetMap(*fh.Root, &newOffsetsBtree.UpdateDict)
        // Delete PK from btree structure
            // btree.Delete(root, current, )
        return nil
    }
    return nil
}







