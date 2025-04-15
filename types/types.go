package types


type Database_t struct {
    Name string
    Tables []Table_t
    NumOfTables int16
}


type Table_t struct {
    Name string
    NumOfColumns int32
    Columns [] Column_t
    Entries [] byte
}




type Column_t struct {
    Name string
    Type Type_t
    Size int16
}


type Type_t uint8

const (
    INT Type_t = iota
    VARCHAR 
    FLOAT 
    BOOL
    NONE
)

var typeNames = map[Type_t] string {
    INT: "INTEGER",
    VARCHAR: "VARCHAR",
    FLOAT: "FLOAT",
    BOOL: "BOOL",
    NONE: "NONE",
}

func (t Type_t) String() string {
    return typeNames[t]
}



