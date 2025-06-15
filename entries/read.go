package entries

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/MalikL2005/SeliaDB-II/types"
	"github.com/MalikL2005/SeliaDB-II/btree"
)

func ReadTableFromFile (path string) (*types.Table_t, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    // Read table
    tb := types.Table_t{}
    if err = binary.Read(f, binary.LittleEndian, &tb.NumOfColumns); err != nil {
        fmt.Println("Err")
        fmt.Println(err)
        return nil, err
    }
    

    bytes, err := ReadStringFromFile (f, types.MAX_TABLE_NAME_LENGTH)
    if err != nil {
        return nil, err
    }
    tb.Name = string(bytes)

    curPos, _ := f.Seek(0, 1)
    fmt.Println("before reading eotd:", curPos)
    if err = binary.Read(f, binary.LittleEndian, &tb.EndOfTableData); err != nil {
        return nil, err
    }
    fmt.Println("reading this as eotd", tb.EndOfTableData)
    curPos, _ = f.Seek(0, 1)

    fmt.Println("before reading start entries", curPos)
    if err = binary.Read(f, binary.LittleEndian, &tb.StartEntries); err != nil {
        return nil, err
    }
    fmt.Println("reading this as start entries", tb.StartEntries)

    tb.Columns = make([]types.Column_t, tb.NumOfColumns)
    // read columns
    fmt.Println(tb.NumOfColumns)
    for i := range tb.NumOfColumns {
        offset, err := f.Seek(0, 1)
        if err != nil {
            return nil, err
        }
        tb.Columns[i], err = ReadColumnFromFile(f, offset)
        if err != nil {
            fmt.Println(err)
        }
        if tb.Columns[i].Indexed {
            _, err = btree.ReadIndexFromFile(tb.Name, tb.Columns[i].Name)
            if err != nil {
                return nil, err
            }
        }
    }
    return &tb, nil
}




func ReadStringFromFile (f *os.File, MAX_LEN int) ([]byte, error) {
    var bytes []byte
    buf := make([]byte, 1)
    for range MAX_LEN {
        _, err := f.Read(buf)
        if err != nil && err.Error() == "EOF" {
        return bytes, nil
        } else if err != nil {
            return nil, err
        }

        if buf[0] == 0 {
            break
        }
        bytes = append(bytes, buf[0])
    }
    return bytes, nil
}



func ReadColumnFromFile (f * os.File, offset int64) (types.Column_t, error) {
    _, err := f.Seek(offset, 0)
    if err != nil {
        fmt.Println("Error moving cursor in file")
        return types.Column_t{}, err
    }

    colBuffer := types.Column_t{}
    buf, err := ReadStringFromFile(f, types.MAX_COLUMN_NAME_LENGTH)
    if err != nil {
        fmt.Println("Error reading colname")
        return types.Column_t{}, err
    }
    colBuffer.Name = string(buf)

    err = binary.Read(f, binary.LittleEndian, &colBuffer.Type)
    if err != nil {
        fmt.Println("Error reading coltype")
        fmt.Println(err)
        return types.Column_t{}, err
    }

    err = binary.Read(f, binary.LittleEndian, &colBuffer.Size)
    if err != nil {
        fmt.Println("Error reading colsize")
        fmt.Println(err)
        return types.Column_t{}, err
    }

    err = binary.Read(f, binary.LittleEndian, &colBuffer.Indexed)
    if err != nil {
        fmt.Println("Error reading colIndex")
        fmt.Println(err)
        return types.Column_t{}, err
    }

    return colBuffer, nil
}
