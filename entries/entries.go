package entries

import (
	"encoding/binary"
    "math"
	"errors"
	"fmt"
	"os"

	"github.com/MalikL2005/SeliaDB-II/types"
)

func AddEntry (tb *types.Table_t, values ... any) error {
    if tb.Entries == nil {
        tb.Entries = &types.Entries_t{}
    }
    if len(values) != int(tb.NumOfColumns) {
        return fmt.Errorf("Must pass correct number of arguments. Expected %d, got %d", tb.NumOfColumns, len(values))
    }
    var entry []byte
    var err error
    for i, col := range tb.Columns {
        switch col.Type {
        case types.VARCHAR:
            s, ok := values[i].(string)
            if ok {
                fmt.Println("string:", s)
                entry = append(entry, s+"\000"...)
            }
        case types.INT32:
            n, ok := values[i].(int32)
            if ok {
                fmt.Println("int32:", int32(n))
                entry, err = binary.Append(entry, binary.LittleEndian, int32(n))
                if err != nil {
                    return err
                }
            }
        case types.FLOAT32:
            f, ok := values[i].(float32)
            if ok {
                fmt.Println("float32:", float32(f))
                entry, err = binary.Append(entry, binary.LittleEndian, float32(f))
                if err != nil {
                    return err
                }
            }
        }
    }
    tb.Entries.Values = append(tb.Entries.Values, entry)
    tb.Entries.NumOfEntries ++
    fmt.Println(entry)
    // write entry to file
    err = AppendEntryToFile(tb, entry)
    if err != nil {
        fmt.Println("Error writing entry to file", err)
        return err
    }
    // btree indices


    return nil
}


func ReadEntryIndex (tb *types.Table_t, index int) ([][]byte, error) {
    fmt.Println("Reading entry")
    if tb.Entries == nil {
        return [][]byte{}, errors.New("Entries cannot be Nil")
    }
    if tb.Entries.Values == nil {
        return [][]byte{}, errors.New("Entries->Values cannot be nil")
    }

    if tb.Entries.NumOfEntries-1 < uint64(index) {
        return [][]byte{}, fmt.Errorf("Entries does not contain %d many values (only %d)", index, tb.Entries.NumOfEntries)
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
            if currentPosition >= len(tb.Entries.Values[index]){
                continue
            }
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



// returns val ([][]byte), offset to next entry (absolute to file_start)
func ReadEntryFromFile (tb *types.Table_t, offset int) ([][]byte, int64, error) {
    fmt.Println("Reading entry")
    fmt.Println("starting at", offset)
    // if tb.Entries == nil {
    //     return [][]byte{}, 0, errors.New("Entries cannot be Nil")
    // }
    // if tb.Entries.Values == nil {
    //     return [][]byte{}, 0, errors.New("Entries->Values cannot be nil")
    // }

    f, err := os.Open(tb.MetaData.FilePath)
    if err != nil {
        return [][]byte{}, 0, err
    }
    defer f.Close()

    _, err = f.Seek(int64(offset), 0)
    if err != nil {
        return [][]byte{}, 0, err
    }
    
    values := make([][]byte, 0)
    currentPosition := 0
    for _, col := range tb.Columns {
        fmt.Print(col.Name, ": ", col.Type.String(), " (", col.Size, "): ")
        switch col.Type {
        case types.VARCHAR:
            buff := make([]byte, 0)
            buffRead := make([]byte, 1)
            for range col.Size {
                _, err := f.Read(buffRead)
                if err != nil {
                    return [][]byte{}, 0, err
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
        case types.INT32:
            bt := make([]byte, col.Size)
            _, err := f.Read(bt)
            if err != nil {
                return [][]byte{}, 0, err
            }
            currentPosition += int(col.Size)
            val := int32(binary.LittleEndian.Uint32(bt))
            fmt.Print(val, " ")
            fmt.Println(bt)
            values = append(values, bt)
        case types.FLOAT32:
            bt := make([]byte, col.Size)
            _, err := f.Read(bt)
            if err != nil {
                return [][]byte{}, 0, err
            }
            currentPosition += int(col.Size)
            bits := binary.LittleEndian.Uint32(bt)
            val := math.Float32frombits(bits)
            fmt.Print(val, " ")
            fmt.Println(bt)
            values = append(values, bt)
        }
    }

    pos, _ := f.Seek(0, 1)
    fmt.Println("ended entry @", pos)
    // pNextEntry
    pNextEntry := uint8(0)
    err = binary.Read(f, binary.LittleEndian, &pNextEntry)
    if err != nil {
        return [][]byte{}, 0, err
    }
    
    pos, _ = f.Seek(0, 1)
    fmt.Println("ended @", pos)
    fmt.Println("next entry @", pos+int64(pNextEntry)+int64(binary.Size(pNextEntry)))

    return values, int64(pos+int64(pNextEntry)+int64(binary.Size(pNextEntry))), nil
}


func GetEntryLength (entry [][]byte) int {
    entryLength := 0
    for _, row := range entry {
        entryLength += len(row)
    }
    return entryLength
}



