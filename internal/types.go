package internal 


type database_t struct {
    name string
    tables * table_t
    table_metadata table_metadata_t
}


type table_t struct {
    name string
    numOfColumns int
    columns * column_t
}


type table_metadata_t struct {
    types * typeState
}


type column_t struct {
    name string
    values * any
}


type typeState int

const (
    INT typeState = iota
    VARCHAR 
    FLOAT 
    BOOL
    NONE
)

var typeNames = map[typeState] string {
    INT: "INTEGER",
    VARCHAR: "VARCHAR",
    FLOAT: "FLOAT",
    BOOL: "BOOL",
    NONE: "NONE",
}


func (ts typeState) String() string {
    return typeNames[ts]
}



