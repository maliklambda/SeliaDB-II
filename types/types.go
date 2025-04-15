package types


type Database_t struct {
    Name string
    Tables []Table_t
    NumOfTables int
}


type Table_t struct {
    Name string
    NumOfColumns int64
    Columns [] Column_t
    Entries [] byte
}




type Column_t struct {
    Name string
    Type Type_t
    Size int
}


type Type_t int

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



