package entries

import (
	"encoding/binary"
	"errors"
	"fmt"

	// "reflect"

	"github.com/MalikL2005/Go_DB/read_write"
	"github.com/MalikL2005/Go_DB/types"
)

func AddEntry (tb *types.Table_t, values ... any) error {
    if tb.Entries == nil {
        tb.Entries = &types.Entries_t{}
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
    err := read_write.WriteEntryToFile(tb, "test.bin", entry)
    if err != nil {
        fmt.Println("Error writing entry to file", err)
        return err
    }
    return nil
}


func ReadEntry (tb types.Table_t, index int) error {
    if tb.Entries == nil {
        return errors.New("Entries cannot be Nil")
    }
    if tb.Entries.Values == nil {
        return errors.New("Entries->Values cannot be nil")
    }

    if tb.Entries.NumOfEntries-1 < uint64(index) {
        return errors.New(fmt.Sprintf("Entries does not contain %d many values (only %d)", index, tb.Entries.NumOfEntries))
    }
    fmt.Println(tb.Entries.Values[index])

    currentPosition := 0
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
        } else {
            bt := tb.Entries.Values[index][currentPosition:col.Size]
            currentPosition += int(col.Size)
            val := int32(binary.LittleEndian.Uint32(bt))
            fmt.Print(val, " ")
            fmt.Println(bt)
        }
    }

    return nil
}


