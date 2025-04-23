package entries

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	// "github.com/MalikL2005/Go_DB/btree"
	"github.com/MalikL2005/Go_DB/types"
)

func AddEntry (tb *types.Table_t, fh *FileHandler, values ... any) error {
    if tb.Entries == nil {
        tb.Entries = &types.Entries_t{}
    }
    if len(values) != int(tb.NumOfColumns) {
        return errors.New(fmt.Sprintf("Must pass correct number of arguments. Expected %d, got %d", tb.NumOfColumns, len(values)))
    }
    var entry []byte
    for i := range tb.NumOfColumns {
        // fmt.Println(reflect.TypeOf(values[i]))
        s, ok := values[i].(string)
        if ok {
            fmt.Println("string:", s)
            entry = append(entry, s+"\000"...)
        }
        n, ok := values[i].(int32)
        if ok {
            fmt.Println("int32:", int32(n))
            entry = binary.LittleEndian.AppendUint32(entry, uint32(n))
        }
    }
    tb.Entries.Values = append(tb.Entries.Values, entry)
    tb.Entries.NumOfEntries ++
    fmt.Println(entry)
    err := AppendEntryToFile(tb, fh, entry)
    if err != nil {
        fmt.Println("Error writing entry to file", err)
        return err
    }
    return nil
}


func ReadEntryIndex (tb types.Table_t, index int) ([][]byte, error) {
    fmt.Println("Reading entry")
    if tb.Entries == nil {
        return [][]byte{}, errors.New("Entries cannot be Nil")
    }
    if tb.Entries.Values == nil {
        return [][]byte{}, errors.New("Entries->Values cannot be nil")
    }

    if tb.Entries.NumOfEntries-1 < uint64(index) {
        return [][]byte{}, errors.New(fmt.Sprintf("Entries does not contain %d many values (only %d)", index, tb.Entries.NumOfEntries))
    }
    fmt.Println(tb.Entries.Values[index])

    currentPosition := 0
    var values [][]byte
    for _, col := range tb.Columns {
        fmt.Print(col.Type.String(), " (", col.Size, "): ")
        if col.Type == types.VARCHAR {
            buff := make([]byte, 0)
            for tb.Entries.Values[index][currentPosition] != 0 {
                buff = append(buff, tb.Entries.Values[index][currentPosition])
                currentPosition ++
            }
            // append String termination \0
            buff = append(buff, tb.Entries.Values[index][currentPosition])
            currentPosition ++
            fmt.Print(string(buff), " ")
            fmt.Println(buff)
            values = append(values, buff)
        } else {
            bt := tb.Entries.Values[index][currentPosition:col.Size]
            currentPosition += int(col.Size)
            val := int32(binary.LittleEndian.Uint32(bt))
            fmt.Print(val, " ")
            fmt.Println(bt)
            values = append(values, bt)
        }
    }

    return values, nil
}


func ReadEntryFromFile (tb *types.Table_t, offset int, fh *FileHandler) ([][]byte, error) {
    fmt.Println("Reading entry")
    fmt.Println("starting at", offset)
    if tb.Entries == nil {
        return [][]byte{}, errors.New("Entries cannot be Nil")
    }
    if tb.Entries.Values == nil {
        return [][]byte{}, errors.New("Entries->Values cannot be nil")
    }

    f, err := os.Open(fh.Path)
    if err != nil {
        return [][]byte{}, err
    }
    defer f.Close()

    _, err = f.Seek(int64(offset), 0)
    if err != nil {
        return [][]byte{}, err
    }
    
    values := make([][]byte, 0)
    currentPosition := 0
    for _, col := range tb.Columns {
        fmt.Print(col.Type.String(), " (", col.Size, "): ")
        if col.Type == types.VARCHAR {
            buff := make([]byte, 0)
            buffRead := make([]byte, 1)
            for range col.Size {
                _, err := f.Read(buffRead)
                if err != nil {
                    return [][]byte{}, err
                }
                if buffRead[0] == 0 {
                    break
                }
                buff = append(buff, buffRead[0])
                currentPosition ++
            }
            // append String termination \0
            buff = append(buff, buffRead[0])
            currentPosition ++
            fmt.Print(string(buff), " ")
            fmt.Println(buff)
            values = append(values, buff)
        } else if col.Type == types.INT32 {
            bt := make([]byte, col.Size)
            _, err := f.Read(bt)
            if err != nil {
                return [][]byte{}, err
            }
            currentPosition += int(col.Size)
            val := int32(binary.LittleEndian.Uint32(bt))
            fmt.Print(val, " ")
            fmt.Println(bt)
            values = append(values, bt)
        }
    }
    pos, _ := f.Seek(0, 1)
    fmt.Println("ended at", pos)

    return values, nil


}


func GetEntryLength (entry [][]byte) int {
    entryLength := 0
    for _, row := range entry {
        entryLength += len(row)
    }
    return entryLength
}




