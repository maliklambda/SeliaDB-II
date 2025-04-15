package types


type Database_t struct {
    Name string
    Tables []Table_t
    NumOfTables uint16
}


type Table_t struct {
    Name string
    NumOfColumns uint32
    OffsetToLastEntry uint64
    StartEntries uint16
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



