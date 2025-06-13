package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"unsafe"
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
    Indeces [] Index_t

    // this part is not written to file, only kept in memory
    MetaData TableMetaData_t
}

type TableMetaData_t struct {
    FilePath string
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


type Index_t struct {
    ColIndex uint32
    Root * any // * btree.Node_t
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
    START_ENTRIES = 1000
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

var CompareStrings = map[string]CompareOperator{
    ">": GREATER,
    "<": SMALLER,
    "=": EQUAL,
    "<=": SMALLER_EQUAL,
    ">=": GREATER_EQUAL,
}

func GetCompareOperator (cmpString string) CompareOperator {
    return CompareStrings[cmpString]
}


type CompareConnector uint8

const (
    AND CompareConnector = iota
    OR
    MISSING_CONNECTOR
)


type CompareObj struct {
    ColName string
    CmpOperator CompareOperator
    Value any
    Connector CompareConnector // this should default to AND as every AND-condition must be fulfilled
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


// return  1 if v1 is greater
//        -1 if v2 is greater
//         0 if v1 equals v2
func CompareAnyValues (v1, v2 any, tp Type_t) (int, error){
    switch tp {
    case INT32:
        i1, ok := v1.(int32)
        if !ok {
            return 0, errors.New("Expected type INT32.")
        }
        i2, ok := v2.(int32)
        if !ok {
            return 0, errors.New("Expected type INT32.")
        }
        if i1 > i2 {
            return 1, nil
        } else if i1 < i2 {
            return -1, nil
        }
        return 0, nil
    case FLOAT32:
        f1, ok := v1.(float32)
        if !ok {
            return 0, errors.New("Expected type FLOAT32.")
        }
        f2, ok := v2.(float32)
        if !ok {
            return 0, errors.New("Expected type FLOAT32.")
        }
        if f1 > f2 {
            return 1, nil
        } else if f1 < f2 {
            return -1, nil
        }
        return 0, nil
    case VARCHAR:
        s1, ok := v1.(string)
        if !ok {
            return 0, errors.New("Expected type VARCHAR.")
        }
        s2, ok := v2.(string)
        if !ok {
            return 0, errors.New("Expected type VARCHAR.")
        }
        fmt.Println("comparing '", s1, "' and '", s2, "'")
        ret := strings.Compare(s1, s2+"\000")
        fmt.Println(ret)
        fmt.Println([]byte(s1))
        fmt.Println([]byte(s2))
        return ret, nil
    }
    return 0, errors.New("Invalid type")
}


func (tb Table_t) IsColIndexed (ColIndex uint32) bool {
    for _, col := range tb.Indeces {
        if col.ColIndex == ColIndex{
            return true
        }
    }
    return false
}


func (tb Table_t) FindIndex (index uint32) (uint32, error) {
    for i, col := range tb.Indeces {
        if col.ColIndex == index {
            return uint32(i), nil
        }
    }
    return uint32(0), errors.New("Index provided is not in tables' indices")
}


func ByteSliceToValue (bytes []byte, tp Type_t) (any, error) {
    switch tp {
    case VARCHAR: return string(bytes), nil
    case INT32: return int32(binary.LittleEndian.Uint32(bytes)), nil
    case FLOAT32: 
        bits := (binary.LittleEndian.Uint32(bytes))
        return math.Float32frombits(bits), nil
    }
    return nil, errors.New(fmt.Sprint("Not yet supported type ", tp.String()))
}



type JoinType uint8

const (
    INNER JoinType = iota
    OUTER
    LEFT 
    RIGHT
    MISSING_JOIN_TYPE
)


type Join_t map[string] struct {
    Left string
    Right string
    How JoinType
}


var JoinTypeStrings= map[string]JoinType{
    "INNER": INNER,
    "OUTER": OUTER,
    "LEFT ": LEFT,
    "RIGHT": RIGHT,
}

func GetJoinType(s string) JoinType {
    jt, ok := JoinTypeStrings[s]
    if !ok {
        return MISSING_JOIN_TYPE
    }
    return jt
}
