package main

import (
	"fmt"

	"github.com/MalikL2005/Go_DB/types"
	"github.com/MalikL2005/Go_DB/write"
)


func main (){
    col1 := types.Column_t {
        Name: "col1",
        Type: types.INT,
        Size: 4,
    }
    col2 := types.Column_t {
        Name: "column2",
        Type: types.VARCHAR,
        Size: 255,
    }

    tb1 := types.Table_t {
        Name: "tb1",
        NumOfColumns: 1023,
        Columns: []types.Column_t{col1, col2},
    }
    fmt.Println(tb1.Columns)
    fmt.Println(tb1.Entries)
    fh, err := write.OpenFile("test.bin")
    if err != nil {
        fmt.Println(err)
        panic(1)
    }

    err = fh.WriteTableToFile(tb1, 0)
    if err != nil {
        fmt.Println(err)
        panic(1)
    }
    tb2 := types.Table_t{}
    fh.ReadFromFile(&tb2, 0)
    fmt.Println(tb1)
    fmt.Println(tb2)
}

