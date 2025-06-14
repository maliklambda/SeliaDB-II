package entries

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

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


    posEndTableData, _ := f.Seek(0, 1)
    fmt.Println("before writing end tb data", posEndTableData)
    err = binary.Write(f, binary.LittleEndian, tb.EndOfTableData)
    if err != nil {
        return err
    }
    fmt.Println("Writing this as tb EndOfTableData", tb.EndOfTableData)

    posStartEntries, _ := f.Seek(0, 1)
    fmt.Println("before writing start entries", posStartEntries)
    err = binary.Write(f, binary.LittleEndian, tb.StartEntries)
    if err != nil {
        return err
    }
    fmt.Println("Writing this as tb start entries", tb.StartEntries)

    for _, col := range tb.Columns {
        offset, err := f.Seek(0,1)
        if err != nil {
            fmt.Println("Could not write this column to file.")
            continue
        }
        fmt.Printf("Offset: %d\n", offset)
        fmt.Println("column:", col)
        fmt.Println(f)
        err = WriteColumnToFile(col, offset, f)
        if err != nil {
            return err
        }

    }
    pos, err := f.Seek(0, 1)
    if err != nil {
        return err
    }
    fmt.Println("Offset to entry-start: ", pos)
    tb.EndOfTableData = uint16(pos)

    _, err = f.Seek(posEndTableData, 0)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, tb.EndOfTableData)
    if err != nil {
        return err
    }
    fmt.Println("after: Writing this as tb EndOfTableData", tb.EndOfTableData)
    fmt.Println("after: Writing at", posEndTableData)

    _, err = f.Seek(0, 2)
    if err != nil {
        return err
    }
    _, err = f.Write([]byte(strings.Repeat("\000", types.GetTableDataBuffer())))
    if err != nil {
        return err
    }

    pos, err = f.Seek(0, 2)
    if err != nil {
        return err
    }
    fmt.Println("\n\nHere we have", pos)
    tb.StartEntries = uint16(pos)
    _, err = f.Seek(posStartEntries, 0)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, tb.StartEntries)
    if err != nil {
        return err
    }
    fmt.Println("after: Writing this as StartEntries", tb.StartEntries)
    fmt.Println("after: Writing at", posStartEntries)

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

    err = binary.Write(f, binary.LittleEndian, col.Indexed)
    if err != nil {
        fmt.Println("Error writing col indexed to file")
        return err
    }
    return nil

}




func AppendEntryToFileOne (tb *types.Table_t, entry []byte) error {
    fmt.Println("Writing entry to file")
    fmt.Println(entry)
    fmt.Println(tb.MetaData.FilePath)
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR, 0644)
    if err != nil {
        return err
    }
    pos, err := f.Seek(0, 1)
    if err != nil {
        return err
    }
    fmt.Print("\n\n")
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
        InsertToBtree(&node, int32(val), uint32(pos+int64(types.GetEntryBuffer())), types.INT32)
        
    }

    p,_ := f.Seek(0, 1)
    fmt.Println("writing entry @", p)
    _, err = f.Write(entry)
    if err != nil {
        return err
    }
    _, err = f.Write([]byte(strings.Repeat("\000", types.GetEntryBuffer())))

    fmt.Println("Wrote entry successfully")
    return nil
}



func UpdateEndOfTableData (path string, newEndOfTableData uint16) error {
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

    // now at correct pos in file
    pos, err := f.Seek(0, 1)
    if err != nil {
        return err
    }
    var oldEndOfTableData uint16
    err = binary.Read(f, binary.LittleEndian, &oldEndOfTableData)
    if err != nil {
        return err
    }

    fmt.Println(pos)

    _, err = f.Seek(int64(binary.Size(uint16(1))), 1)
    if err != nil {
        return err
    }

    // Read StartEntries
    var StartEntries uint16
    err = binary.Read(f, binary.LittleEndian, &StartEntries)
    if err != nil {
        return err
    }

    if StartEntries <= newEndOfTableData {
        fmt.Println("doubling eot-buffer-length")
        StartEntries += uint16(types.GetTableDataBuffer())
        err = types.AllocateInFile(path, int64(oldEndOfTableData), int64(types.GetTableDataBuffer()))
        if err != nil {
            return err
        }
    }

    // write new eot data
    _, err = f.Seek(pos, 0)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, newEndOfTableData)
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



func AppendEntryToFile (tb *types.Table_t, entry []byte) error {
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR, 0644)
    if err != nil {
        return err
    }

    p, _ := f.Seek(0, 2)
    fmt.Println("buffer len:", types.GetEntryBuffer())
    fmt.Print("Writing buffer @", p, "\n\n")
    _, err = f.Write([]byte(strings.Repeat("\000", types.GetEntryBuffer())))
    if err != nil {
        return err
    }

    p, _ = f.Seek(0, 2)
    fmt.Println("Entry length:", len(entry))
    fmt.Print("Writing entry @", p, "\n\n")
    _, err = f.Write(entry)
    if err != nil {
        return err
    }

    // write pointer to next entry (i.e. 0 as it is the last entry)
    err = binary.Write(f, binary.LittleEndian, uint16(types.GetEntryBuffer()))
    if err != nil {
        return err
    }
    p, _ = f.Seek(0, 1)
    fmt.Println("cur:", p)
    fmt.Println("pNextEntry:", types.GetEntryBuffer())

    return nil
}



