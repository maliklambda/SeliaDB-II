package entries

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/MalikL2005/Go_DB/btree"
	"github.com/MalikL2005/Go_DB/types"
)



type FileHandler struct {
    Path string
    Root **btree.Node_t
    File *os.File
}



func CreateFile (fileName string) (FileHandler, error) {
    f, err := os.Create(fileName)
    if err != nil {
        return FileHandler{}, err
    }
    defer f.Close()
    newRoot := &btree.Node_t{}
    return FileHandler{fileName, &newRoot, nil}, nil
}




func WriteTableToFile (tb *types.Table_t, fh FileHandler) error {
    fmt.Println("Writing", fh.Path)
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    fh.File = f
    defer f.Close()
    // move cursor to SOF + offset
    _, err = f.Seek(0, 0)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, tb.NumOfColumns)
    if err != nil {
        return err
    }

    _, err = f.Write([]byte(tb.Name + "\000"))
    if err != nil {
        return err
    }


    err = binary.Write(f, binary.LittleEndian, tb.StartEntries)
    if err != nil {
        return err
    }

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
        fh.WriteColumnToFile(col, offset)
    }
    pos, err := f.Seek(0, 1)
    if err != nil {
        return err
    }
    fmt.Println("Offset to entry-start: ", pos)
    tb.StartEntries += uint16(pos)

    err = binary.Write(f, binary.LittleEndian, tb.StartEntries)
    if err != nil {
        return err
    }

    tb.OffsetToLastEntry = 0
    if tb.Entries != nil {
        fmt.Println("Priting entries")
        for i := range tb.Entries.NumOfEntries {
            fmt.Println(tb.Entries.Values[i])
            AppendEntryToFile(tb, fh, tb.Entries.Values[i])
            tb.OffsetToLastEntry += uint64(len(tb.Entries.Values[i]))
            fmt.Println("new offset:", tb.OffsetToLastEntry)
        }
    }
    return nil
}



func (fh FileHandler) WriteColumnToFile (col types.Column_t, offset int64) error{
    fmt.Println("Writing this right here to file", fh.Path, ":", col)
    fmt.Println(fh.File)
    _, err := fh.File.Write([]byte(col.Name + "\000"))
    if err != nil {
        fmt.Println("Error writing col name to file")
        return err
    }

    err = binary.Write(fh.File, binary.LittleEndian, col.Type)
    if err != nil {
        fmt.Println("Error writing col type to file")
        fmt.Println(err)
        return err
    }

    err = binary.Write(fh.File, binary.LittleEndian, col.Size)
    if err != nil {
        fmt.Println("Error writing col size to file")
        return err
    }
    return nil

}


func AppendEntryToFile (tb *types.Table_t, fh FileHandler, entry []byte) error {
    fmt.Println("Writing entry to file")
    fmt.Println(entry)
    fmt.Println(fh.Path)
    f, err := os.OpenFile(fh.Path, os.O_RDWR, 0644)
    if err != nil {
        return err
    }
    if tb.OffsetToLastEntry == 0 {
        tb.OffsetToLastEntry = uint64(tb.StartEntries)
    }
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
    // take id as first column
    val := binary.LittleEndian.Uint32(entry)

    // insert into btree
    fmt.Println("Inserting offset:", pos, "Key:", val)
    InsertToBtree(fh.Root, val, uint32(pos))
    _, err = f.Write(entry)
    if err != nil {
        return err
    }
    tb.OffsetToLastEntry += uint64(len(entry))

    fmt.Println(tb.OffsetToLastEntry)
    fmt.Println("Wrote entry successfully")
    UpdateOffsetLastEntry(fh, 0, uint16(len(entry)))
    return nil
}



func UpdateOffsetLastEntry (fh FileHandler, offsetTable int64, newLastEntryOffset uint16) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }

    _, err = f.Seek(offsetTable, 0)
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


