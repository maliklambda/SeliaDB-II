package entries

import (
	"encoding/binary"
	"fmt"
	"os"
    "errors"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/types"
)


func WriteTableToFile (tb *types.Table_t) error {
    fmt.Println("Writing", tb.MetaData.FilePath)
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    err = binary.Write(f, binary.LittleEndian, tb.NumOfColumns)
    if err != nil {
        return err
    }

    _, err = f.Write([]byte(tb.Name + "\000"))
    if err != nil {
        return err
    }


    posStartEntries, _ := f.Seek(0, 1)
    fmt.Println("before writing start entries", posStartEntries)
    err = binary.Write(f, binary.LittleEndian, tb.StartEntries)
    if err != nil {
        return err
    }
    fmt.Println("Writing this as starentries", tb.StartEntries)

    fmt.Println("Writing this as offset to last entry", tb.OffsetToLastEntry)
    err = binary.Write(f, binary.LittleEndian, tb.OffsetToLastEntry)
    if err != nil {
        return err
    }

    for _, col := range tb.Columns {
        offset, err := f.Seek(0,1)
        if err != nil {
            fmt.Println("Could not write this column to file.")
            continue
        }
        fmt.Printf("Offset: %d\n", offset)
        fmt.Println("column:", col)
        fmt.Println(f)
        WriteColumnToFile(col, offset, f)

    }
    pos, err := f.Seek(0, 1)
    if err != nil {
        return err
    }
    fmt.Println("Offset to entry-start: ", pos)
    tb.StartEntries = uint16(pos)

    _, err = f.Seek(posStartEntries, 0)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, tb.StartEntries)
    if err != nil {
        return err
    }
    err = DeleteAllEntries(tb)
    if err != nil {
        fmt.Println("Error deleting entries:", err)
        return err
    }

    tb.OffsetToLastEntry = 0
    fmt.Println("starting offset:", tb.StartEntries)
    if tb.Entries != nil {
        fmt.Println("Priting entries")
        for i := range tb.Entries.NumOfEntries {
            fmt.Println(tb.Entries.Values[i])
            AppendEntryToFile(tb, tb.Entries.Values[i])
            fmt.Println("new offset:", tb.OffsetToLastEntry)
        }
    }
    return nil
}



func WriteColumnToFile (col types.Column_t, offset int64, f * os.File) error{
    if f == nil {
        return errors.New("File must not be nil")
    }
    fmt.Println("Writing this right here to file", ":", col)
    _, err := f.Write([]byte(col.Name + "\000"))
    if err != nil {
        fmt.Println("Error writing col name to file")
        return err
    }

    err = binary.Write(f, binary.LittleEndian, col.Type)
    if err != nil {
        fmt.Println("Error writing col type to file")
        fmt.Println(err)
        return err
    }

    err = binary.Write(f, binary.LittleEndian, col.Size)
    if err != nil {
        fmt.Println("Error writing col size to file")
        return err
    }
    return nil

}


func AppendEntryToFile (tb *types.Table_t, entry []byte) error {
    fmt.Println("Writing entry to file")
    fmt.Println(entry)
    fmt.Println(tb.MetaData.FilePath)
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR, 0644)
    if err != nil {
        return err
    }
    if tb.OffsetToLastEntry == 0 {
        tb.OffsetToLastEntry = uint64(tb.StartEntries)
    }
    fmt.Println("Currently right here", tb.OffsetToLastEntry)
    pos, err := f.Seek(int64(tb.OffsetToLastEntry), 0)
    if err != nil {
        return err
    }
    fmt.Print("\n\n")
    fmt.Println("at position:", tb.OffsetToLastEntry + uint64(tb.StartEntries))
    fmt.Println("last entry:", tb.OffsetToLastEntry)
    fmt.Println("StartEntries:", uint64(tb.StartEntries))
    fmt.Println("num entries", tb.Entries.NumOfEntries)
    fmt.Println(tb.Entries.Values)
    // insert into btree
    val := binary.LittleEndian.Uint32(entry)

    // iterate over indices
    for i, index := range tb.Indeces {
        fmt.Println(index)
        fmt.Println(tb.Columns[i].Type)
        node := btree.UnsafePAnyToPNode_t(index.Root)
        InsertToBtree(&node, int32(val), uint32(pos), types.INT32)
        
    }

    _, err = f.Write(entry)
    if err != nil {
        return err
    }
    tb.OffsetToLastEntry += uint64(len(entry))

    fmt.Println(tb.OffsetToLastEntry)
    fmt.Println("Wrote entry successfully")
    UpdateOffsetLastEntry(tb.MetaData.FilePath, uint16(len(entry)))
    return nil
}



func UpdateOffsetLastEntry (path string, newLastEntryOffset uint16) error {
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }

    _, err = f.Seek(int64(binary.Size(uint32(1))), 1)
    if err != nil {
        return err
    }

    // Read table name
    buf := make([]byte, 1)
    for range types.MAX_COLUMN_NAME_LENGTH +1 {
        _, err = f.Read(buf)
        if err != nil {
            return err
        }
        if buf[0] == 0 {
            break
        }
    }

    _, err = f.Seek(int64(binary.Size(uint16(1))), 1)
    if err != nil {
        return err
    }

    pos, _ := f.Seek(0, 1)
    fmt.Println(pos)
    err = binary.Write(f, binary.LittleEndian, newLastEntryOffset)
    if err != nil {
        return err
    }
    return nil
}



func UpdateNumOfColumns (path string, newNumOfColumns uint32) error {
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    if err := binary.Write(f, binary.LittleEndian, newNumOfColumns); err != nil {
        return err
    }

    return nil
}


func UpdateStartEntries (path string, newStartEntries uint16) error {
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    _, err = f.Seek(int64(binary.Size(uint32(1))), 1)
    if err != nil {
        return err
    }

    // Read table name
    buf := make([]byte, 1)
    for range types.MAX_COLUMN_NAME_LENGTH +1 {
        _, err = f.Read(buf)
        if err != nil {
            return err
        }
        if buf[0] == 0 {
            break
        }
    }

    if err := binary.Write(f, binary.LittleEndian, newStartEntries); err != nil {
        return err
    }
    return nil
}


// this works with all fixed sized variables (NOT with strings, etc.) -> see WriteStringToFile()
func WriteDataToFile (path string, offset int64, value any) error {
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    _, err = f.Seek(offset, 1) // if bug occurs, change to (offset, 0)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, value)
    if err != nil {
        return err
    }
    return nil
}



// expects string without \000 at the end
func WriteStringToFile (path string, offset int64, s string) error {
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        fmt.Println("err 1")
        return err
    }
    defer f.Close()

    _, err = f.Seek(offset, 0)
    if err != nil {
        fmt.Println("err 2")
        return err
    }

    fmt.Println("writing", len([]byte(s+"\000")))
    fmt.Println(s+"\000")
    _, err = f.Write([]byte(s + "\000"))
    if err != nil {
        fmt.Println("here")
        return err
    }
    return nil

}



