package types

import (
	"encoding/binary"
	"slices"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"unsafe"
	"strconv"
)

type Database_t struct {
    Name string
    Tables []*Table_t
    NumOfTables uint16
}


type Table_t struct {
    NumOfColumns uint32
    Name string
    EndOfTableData uint16
    StartEntries uint16
    Columns [] Column_t
    Entries *Entries_t

    // this part is not written to file, only kept in memory
    Indeces [] Index_t
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
    Indexed bool
}


type Values_t [][][]byte

type MaxLengths_t []int


type Index_t struct {
    ColIndex uint32
    Root * any // * btree.Node_t
}

var tableDataBuffer int = 50
func GetTableDataBuffer () int{
    return tableDataBuffer
}

var entryBuffer int = 50

func GetEntryBuffer () int{
    return entryBuffer
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
    BOOL: binary.Size(true),
    NONE: binary.Size(true),
}

func (t Type_t) String() string {
    return typeNames[t]
}

var PNextEntrySize int64 = int64(binary.Size(uint16(0)))


func StringToType_t (s string) (Type_t, error){
    s = strings.ToUpper(strings.TrimSpace(s))
    for key, val := range typeNames {
        if val == s {
            return key, nil
        }
    }
    return NONE, errors.New("Type does not exist")
}


func (tp Type_t) GetTypeSize (varCharLen uint32) (uint16, error) {
    if tp == VARCHAR {
        if varCharLen == 0 {
            return 0, errors.New("Varchar type with length 0")
        }
        return uint16(varCharLen), nil
    }
    size, ok := typeSizes[tp]
    if !ok {
        return 0, errors.New("No size for this type")
    }
    return uint16(size), nil
}


func (tp Type_t) GetTypeParser () func ([]byte) (any, error) {
		switch tp {
				case INT32: return BytesToInt32
				case VARCHAR: return BytesToVarChar
		}
		return nil
}


const (
    MAX_DATABASE_NAME_LENGTH = 50 // must not be longer than entry/tabledata buffers
    MAX_TABLE_NAME_LENGTH = 50
    MAX_COLUMN_NAME_LENGTH = 50
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
        fmt.Println(v2)
        v1 := string(val1) // handle conversion error: missmatched types
        if strings.HasPrefix(v2, "'") && strings.HasSuffix(v2, "'"){
            v2 = v2[1:len(v2)-1]
        }
        fmt.Printf("comparing here '%s' and '%s'\n\n", v1, v2)
        return strings.Compare(v1, v2+"\000"), nil
    }
    return 0, nil
}




func (col Column_t) GetColSize () int {
    size := len(col.Name+"\000")
    size += binary.Size(col.Type)
    size += binary.Size(col.Size)
    size += binary.Size(col.Indexed)
    return size
}


type CompareOperator uint8

const (
    GREATER CompareOperator = iota
    SMALLER
    EQUAL
    SMALLER_EQUAL
    GREATER_EQUAL
    STARTS_WITH
    ENDS_WITH
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
        case STARTS_WITH: return compareResult == 0
        case ENDS_WITH: return compareResult == 0
    }
    return false
}



func GetOffsetToFirstColumn (tb *Table_t) (int64, error){
    offset := int(unsafe.Sizeof(tb.NumOfColumns)) // NumOfColumns uint32
    offset += len([]byte(tb.Name+"\000")) // Name string
    offset += int(unsafe.Sizeof(tb.StartEntries)) // StartEntries uint16
    offset += int(unsafe.Sizeof(tb.EndOfTableData)) // EndOfTableData uint16
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
						fmt.Println("error i1")
            return 0, errors.New("Expected type INT32.")
        }
        i2, ok := v2.(int32) // this may look weird, but for some reason both cases (valid) occur
        if !ok {
						i2_str := v2.(string)
						i2_int, err := strconv.Atoi(i2_str)
						if err != nil {
								fmt.Println("error i2.2")
								return 0, errors.New("Expected type INT32.")
						}
						i2 = int32(i2_int)
        }
        fmt.Println("comparing", i1, i2)
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
		LEFT_OUTER
		RIGHT_OUTER
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



func IsColIndexed (tb * Table_t, colName string) (bool, int, error) {
    colNames := make([]string, len(tb.Columns))
    for i, col := range tb.Columns {
        colNames[i] = col.Name
    }
    if iCol := slices.Index(colNames, colName); iCol == -1 {
        return false, 0, fmt.Errorf("Column %s does not exist in table %s.", colName, tb.Name)
    } else {
        return tb.Columns[iCol].Indexed, iCol, nil
    }
}


// this returns also the index of the 
func IsColIndexedSlice (tbs * []Table_t, colName string) (isIndexed bool, i_tb int, i_col int, err error) {
		var table_names []string // tables_string for err-msg
		for i, tb := range *tbs {
				left_join_col_name := Strip_table_name(colName, tb.Name)
				isIndexed, i_col, err = IsColIndexed(&tb, left_join_col_name)
				if isIndexed {
						return true, i, i_col, nil
				}
				table_names = append(table_names, tb.Name)
		}
		return false, -1, -1, fmt.Errorf("Column %s does not exist in tables %s.", colName, table_names)
}


func BytesToInt32 (b []byte) (any, error) {
		var res int32
			err := binary.Read(bytes.NewReader(b), binary.NativeEndian, &res)
		if err != nil {
				return 0, err
		}
		return res, nil
}



func BytesToVarChar (bytes []byte) (any, error) {
		return string(bytes), nil
}



// remove tablename from right_column
// "tb_name.col_name" becomes "col_name"
func Strip_table_name (colName, tableName string) string {
		if strings.HasPrefix(colName, tableName + ".") {
				return colName[len(tableName) + 1:]
		}
		return colName
}


