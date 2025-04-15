package types


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



