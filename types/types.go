package types

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type Database_t struct {
    Name string
    Tables []Table_t
    NumOfTables uint16
}


type Table_t struct {
    NumOfColumns uint32
    Name string
    StartEntries uint16
    OffsetToLastEntry uint64
    Columns [] Column_t
    Entries *Entries_t
}


type Entries_t struct {
    NumOfEntries uint64
    Values [][] byte
}



type Column_t struct {
    Name string
    Type Type_t
    Size uint16
}


type Type_t uint8

const (
    INT32 Type_t = iota
    VARCHAR 
    FLOAT 
    BOOL
    NONE
)

var typeNames = map[Type_t] string {
    INT32: "INT32",
    VARCHAR: "VARCHAR",
    FLOAT: "FLOAT",
    BOOL: "BOOL",
    NONE: "NONE",
}

func (t Type_t) String() string {
    return typeNames[t]
}


const (
    MAX_DATABASE_NAME_LENGTH = 20
    MAX_COLUMN_NAME_LENGTH = 20
)



// Returns 0 if equal
// -1 if v1 is greater
// 1 if v2 is greater
func CompareValues (tp Type_t, val1 []byte, val2 any) (int, error) {
    fmt.Println("Comparing values...")
    switch(tp){
    case INT32:
        v2, ok := val2.(int)
        if !ok {
            return 0, errors.New("Type does not match value")
        }
        v1 := int32(binary.LittleEndian.Uint32(val1))
        fmt.Println("Comparing", v2, v1)
        if v1 > int32(v2) {
            return -1, nil
        } else if int32(v2) > v1 {
            return 1, nil
        } else if v1 == int32(v2) {
            return 0, nil
        }
    }
    return 0, nil
}
