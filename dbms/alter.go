package dbms

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/types"
)


func AlterColumnName (tb *types.Table_t, oldColName, newColName string) error {
    colIndex, err := entries.StringToColumnIndex(tb, oldColName)
    if err != nil {
        return err
    }

    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    var numCols uint32
    if err = binary.Read(f, binary.LittleEndian, &numCols); err != nil {
        return err
    }
    fmt.Println(numCols)

    b, err := entries.ReadStringFromFile(f, types.MAX_TABLE_NAME_LENGTH)
    if err != nil {
        return err
    }
    fmt.Println(b)

    var oldEndOfTableData uint16
    if err = binary.Read(f, binary.LittleEndian, &oldEndOfTableData); err != nil {
        return err
    }

    var oldStartEntries uint16
    if err = binary.Read(f, binary.LittleEndian, &oldStartEntries); err != nil {
        return err
    }

    var curPos int64
    if curPos, err = f.Seek(0, 1); err != nil {
        return err
    }

    fmt.Println("this pos:", curPos)

    var colBuffer types.Column_t
    for range colIndex +1 {
        colBuffer, err = entries.ReadColumnFromFile(f, curPos)
        if err != nil {
            return err
        }
        curPos += int64(colBuffer.GetColSize())
        fmt.Println("reading this column:", colBuffer)
    }
    curPos -= int64(colBuffer.GetColSize()) // after the read, colSize is added to curPos
    f.Close()
    fmt.Println("altering this column:", colBuffer)

    diff := len(newColName) - len(oldColName)
    if diff < 0 {
        fmt.Println("difference between old and new colName", diff)
        if err = types.DeallocateInFile(tb.MetaData.FilePath, curPos, int64(diff*-1)); err != nil {
            return nil
        }
    } else if diff > 0 {
        fmt.Println("difference between old and new colName +", diff)
        if err = types.AllocateInFile(tb.MetaData.FilePath, curPos, int64(diff)); err != nil {
            return nil
        }
    }

    f, err = os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }

    if _, err = f.WriteAt([]byte(newColName+"\000"), curPos); err != nil {
        return err
    }
    f.Close()
    
    // update startentries & oldEndOfTableData
    newEndOfTableData := int(oldEndOfTableData) + diff
    if err = entries.UpdateEndOfTableData(tb, uint16(newEndOfTableData)); err != nil {
        return err
    }

    newStartEntries := int(oldStartEntries) + diff
    if err = entries.UpdateStartEntries(tb, uint16(newStartEntries)); err != nil {
        return err
    }

    return nil
}


