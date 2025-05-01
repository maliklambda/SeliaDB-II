package dbms

import (
	"errors"
	"fmt"
	"os"
    "io"
	"encoding/binary"

	"github.com/MalikL2005/Go_DB/entries"
	"github.com/MalikL2005/Go_DB/types"
)

func AddColumn (fh *entries.FileHandler, tb *types.Table_t, colName string, colType string, varCharLen uint32) error {
    tp, err := types.StringToType_t(colType)
    if err != nil {
        return err
    }
    size, err := tp.GetTypeSize(varCharLen)
    if err != nil {
        return err
    }

    fmt.Println(tp, "size", size)
    fmt.Println(tb.StartEntries)
    if existsColumnName(tb, colName){
        return errors.New("Column name already exists")
    }
    newCol := types.Column_t{
        Name: colName,
        Type: tp,
        Size: size,
    }
    fmt.Println("New column:", newCol)
    insertColumnToFile(fh, tb, &newCol)
    if err = entries.UpdateNumOfColumns(fh, tb.NumOfColumns+1); err != nil {
        return err
    }

    fmt.Println("start entries:", tb.StartEntries)
    colSize := uint16(newCol.GetColSize())
    fmt.Println("offset :", colSize)
    if err = entries.UpdateStartEntries(fh, tb.StartEntries+colSize); err != nil {
        return err
    }

    // Move btree entries

    return nil
}




func existsColumnName (tb *types.Table_t, colName string) bool {
    for _, column := range tb.Columns {
        if column.Name == colName {
            return true
        }
    }
    return false
}


func insertColumnToFile (fh *entries.FileHandler, tb *types.Table_t, col *types.Column_t) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    if err = allocateInFile(fh, int64(tb.StartEntries), int64(col.GetColSize())); err != nil {
        return err
    }
    fmt.Println(f)

    if err = WriteColumnAtOffset(fh, col, int64(tb.StartEntries)); err != nil {
        return err
    }

    return nil
}




func WriteColumnAtOffset (fh *entries.FileHandler, col *types.Column_t, offset int64) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    if _, err := f.Seek(offset, 0); err != nil {
        return err
    }

    if _, err = f.Write([]byte(col.Name+"\000")); err != nil {
        return err
    }

    if err = binary.Write(f, binary.LittleEndian, col.Type); err != nil {
        return err
    }

    if err = binary.Write(f, binary.LittleEndian, col.Size); err != nil {
        return err
    }

    return nil
}



// allocates numBytes many Bytes in file from offset onwards
func allocateInFile (fh *entries.FileHandler, offset int64, numBytes int64) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }

    tmp, err := os.OpenFile("./tmp.tb", os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer tmp.Close()
    // defer os.Remove(tmp.Name())
    
    if _, err := io.CopyN(tmp, f, offset); err != nil {
        return err
    }

    _, err = f.Seek(offset, 0)
    if err != nil {
        return err
    }
    
    _, err = tmp.Seek(offset + numBytes, 0)
    if err != nil {
        return err
    }

    if _, err := io.Copy(tmp, f); err != nil {
        return err
    }

    f.Close()

    err = os.Rename(tmp.Name(), fh.Path)
    if err != nil {
        return err
    }
    
    return nil
}






