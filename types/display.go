package types

import (
	"fmt"
	"strconv"
	"strings"
)

func DisplayByteSlice (bytes [][][]byte, tb *Table_t, maxLengths []int) {
    rowSeparator := "+"
    for i, col := range tb.Columns {
        maxLengths[i] = max(len(col.Name), maxLengths[i])
        rowSeparator += strings.Repeat("-", maxLengths[i]) + "+"
    }
    rowSeparator += "\n"
    fmt.Print(rowSeparator)
    for i, col := range tb.Columns {
        fmt.Print("|")
        spaces := strings.Repeat(" ", maxLengths[i]-len(col.Name))
        fmt.Print(col.Name, spaces)
    }
    fmt.Println("|")
    fmt.Print(rowSeparator)
    for _, entry := range bytes {
        for i, value := range entry {
            fmt.Print("|")
            v, err := ByteSliceToValue(value, tb.Columns[i].Type)
            if err != nil {
                fmt.Print(strings.Repeat(" ", maxLengths[i]))
                continue
            }
            fmt.Print(v)
            fmt.Print(strings.Repeat(" ", (maxLengths[i] - getStdoutLength(v, tb.Columns[i].Type))))
        }
        fmt.Println("|")
    }
    fmt.Print(rowSeparator)
    fmt.Println("Result contains", len(bytes), "rows")
}


func GetDisplayLength (val []byte, tp Type_t) int {
    switch tp {
    case VARCHAR: return len(string(val))
    case INT32: return 10
    }
    return 0
}



func getStdoutLength (v any, tp Type_t) int {
    switch tp {
    case VARCHAR: return max(len(v.(string))-1, 0)
    case INT32: return len(strconv.Itoa(int(v.(int32))))
    }
    return 0
}



func UpdateLongestDisplay (maxLengths []int, bytes [][]byte, tb *Table_t){
    var length int
    for i, col := range tb.Columns {
        length = GetDisplayLength(bytes[i], col.Type)
        if length > maxLengths[i] {
            maxLengths[i] = length
        }
    }
    fmt.Println(maxLengths)
}



