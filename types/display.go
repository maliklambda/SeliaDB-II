package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

)

func DisplayByteSlice (bytes [][][]byte, cols []Column_t, maxLengths []int) {
    fmt.Println(cols)
    if len (bytes) == 0 {
        fmt.Println("Empty set")
        return 
    }
    rowSeparator := "+"
    for i, col := range cols {
        maxLengths[i] = max(len(col.Name), maxLengths[i])
        rowSeparator += strings.Repeat("-", maxLengths[i]+2) + "+"
    }
    rowSeparator += "\n"
    fmt.Print(rowSeparator)
    for i, col := range cols {
        fmt.Print("| ")
        spaces := strings.Repeat(" ", maxLengths[i]-len(col.Name)+1)
        fmt.Print(col.Name, spaces)
    }
    fmt.Println("|")
    fmt.Print(rowSeparator)
    for _, entry := range bytes {
        for i, value := range entry {
            fmt.Print("|")
            v, err := ByteSliceToValue(value, cols[i].Type)
            if err != nil {
                fmt.Print(strings.Repeat(" ", maxLengths[i]))
                continue
            }
            fmt.Print(" ", v)
            fmt.Print(strings.Repeat(" ", (maxLengths[i] - GetDisplayLength(v, cols[i].Type))+1))
        }
        fmt.Println("|")
    }
    fmt.Print(rowSeparator)
    fmt.Println("Result contains", len(bytes), "rows")
}


func GetDisplayLengthByte (val []byte, tp Type_t) int {
    switch tp {
    case VARCHAR: return len(string(val))-1
    case INT32: return len(strconv.Itoa(int(int32(binary.LittleEndian.Uint32(val)))))
    case FLOAT32: 
        f := math.Float32frombits(binary.LittleEndian.Uint32(val))
        return len(strings.TrimRight(fmt.Sprintf("%.4f", f), "0"))
    }
    return 0
}



func GetDisplayLength (v any, tp Type_t) int {
    switch tp {
    case VARCHAR: return max(len(v.(string))-1, 0)
    case INT32: return len(strconv.Itoa(int(v.(int32))))
    case FLOAT32: 
        return len(strings.TrimRight(fmt.Sprintf("%.4f", v.(float32)), "0"))
    }
    return 0
}



func UpdateLongestDisplay (maxLengths []int, bytes [][]byte, cols []Column_t) []int {
    var length int
    for i, col := range cols {
        length = GetDisplayLengthByte(bytes[i], col.Type)
        if length > maxLengths[i] {
            maxLengths[i] = length
        }
    }
    fmt.Println(maxLengths)
    return maxLengths
}



func DisplayErrorMessage (err error) {
    fmt.Println(err.Error())
}
