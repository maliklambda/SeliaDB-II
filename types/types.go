package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
    "unsafe"
    "os"
    "io"
)

type Database_t struct {
    Name string
    Tables []*Table_t
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
    FLOAT32
    BOOL
    NONE
)

var typeNames = map[Type_t] string {
    INT32: "INT32",
    VARCHAR: "VARCHAR",
    FLOAT32: "FLOAT",
    BOOL: "BOOL",
    NONE: "NONE",
}


var typeSizes = map[Type_t] int {
    INT32: binary.Size(int32(0)),
    FLOAT32: binary.Size(float32(0)),
    BOOL: binary.Size(uint8(0)),
    NONE: binary.Size(uint8(0)),
}

func (t Type_t) String() string {
    return typeNames[t]
}


func StringToType_t (tb string) (Type_t, error){
    tb = strings.TrimSpace(tb)
    tb = strings.ToUpper(tb)
    for key, val := range typeNames {
        if val == tb {
            return key, nil
        }
    }
    return NONE, errors.New("Type does not exist")
}


func (tp Type_t) GetTypeSize (varCharLen uint32) (uint16, error) {
    if tp == VARCHAR {
        return uint16(varCharLen +1), nil
    }
    size, ok := typeSizes[tp]
    if !ok {
        return 0, errors.New("No size for this type")
    }
    return uint16(size), nil
}


const (
    MAX_DATABASE_NAME_LENGTH = 20
    MAX_TABLE_NAME_LENGTH = 20
    MAX_COLUMN_NAME_LENGTH = 20
)



// Returns 0 if equal
// -1 if v2 is greater
// 1 if v1 is greater
// first param is the entry (e.g. read from file)
// second param is always the specified value (that is compared to)
func CompareValues (tp Type_t, val1 []byte, val2 any) (int, error) {
    fmt.Println("Comparing values...")
    switch(tp){
    case INT32:
        v2, ok := val2.(int32)
        if !ok {
            return 0, errors.New("Type does not match value")
        }
        v1 := int32(binary.LittleEndian.Uint32(val1))
        fmt.Println("Comparing", v2, v1)
        if v1 > int32(v2) {
            return 1, nil
        } else if int32(v2) > v1 {
            return -1, nil
        } else if v1 == int32(v2) {
            return 0, nil
        }
    case VARCHAR:
        v2, ok := val2.(string)
        if !ok {
            return 0, errors.New("Type does not match value")
        }
        v1 := string(val1) // handle conversion error: missmatched types
        return strings.Compare(v1, v2+"\000"), nil
    }
    return 0, nil
}




func (col Column_t) GetColSize () int {
    size := len(col.Name+"\000")
    size += binary.Size(col.Type)
    size += binary.Size(col.Size)
    return size
}


type CompareOperator uint8

const (
    GREATER CompareOperator = iota
    SMALLER
    EQUAL
    SMALLER_EQUAL
    GREATER_EQUAL
)


type CompareObj struct {
    ColName string
    CmpOperator CompareOperator
    Value any
}



func CompareValuesWithOperator (compareResult int, cmpOperator CompareOperator) bool {
    switch (cmpOperator){
        case GREATER: return compareResult == 1
        case EQUAL: return compareResult == 0
        case SMALLER: return compareResult == -1
        case SMALLER_EQUAL: return (compareResult == -1) || (compareResult == 0)
        case GREATER_EQUAL: return (compareResult == 1) || (compareResult == 0)
    }
    return false
}



func GetOffsetToFirstColumn (tb *Table_t) (int64, error){
    offset := int(unsafe.Sizeof(tb.NumOfColumns)) // NumOfColumns uint32
    offset += len([]byte(tb.Name+"\000")) // Name string
    offset += int(unsafe.Sizeof(tb.StartEntries)) // StartEntries uint16
    offset += int(unsafe.Sizeof(tb.OffsetToLastEntry)) // OffsetToLastEntry uint64
    return int64(offset), nil
}

// datastruct that is used to update the pointers in the btree
type UpdateOffsetList struct {
    Current int32
    UpdateDict map[int] int32 // maps FromOffsetOnwards (uint32) to NumNewBytes (int32)
}



// allocates numBytes many Bytes in file from offset onwards
func AllocateInFile (path string, offset, numBytes int64) error {
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    tmp, err := os.OpenFile("./tmp.tb", os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer tmp.Close()
    // defer os.Remove(tmp.Name())
    
    if _, err := io.CopyN(tmp, f, offset); err != nil {
        return err
    }

    _, err = f.Seek(offset, 0)
    if err != nil {
        return err
    }
    
    _, err = tmp.Seek(offset + numBytes, 0)
    if err != nil {
        return err
    }

    if _, err := io.Copy(tmp, f); err != nil {
        return err
    }

    f.Close()

    err = os.Rename(tmp.Name(), path)
    if err != nil {
        return err
    }
    
    return nil
}



func DeallocateInFile (path string, offset, numBytes int64) error {
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    tmp, err := os.OpenFile("./tmp.tb", os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer tmp.Close()
    // defer os.Remove(tmp.Name())
    
    if _, err := io.CopyN(tmp, f, offset); err != nil {
        return err
    }

    _, err = f.Seek(offset + numBytes, 0)
    if err != nil {
        return err
    }

    if _, err := io.Copy(tmp, f); err != nil {
        return err
    }

    f.Close()

    err = os.Rename(tmp.Name(), path)
    if err != nil {
        return err
    }
    
    return nil
}



