package main

import (
	"fmt"

	"github.com/MalikL2005/Go_DB/types"
    "github.com/MalikL2005/Go_DB/read_write"
    "github.com/MalikL2005/Go_DB/entries"
)


func main (){
    col1 := types.Column_t {
        Name: "col1",
        Type: types.INT32,
        Size: 4,
    }
    col2 := types.Column_t {
        Name: "column2",
        Type: types.VARCHAR,
        Size: 255,
    }

    tb1 := types.Table_t {
        Name: "tb1",
        NumOfColumns: 2,
        Columns: []types.Column_t{col1, col2},
    }
    fmt.Println(tb1.Columns)
    fmt.Println(tb1.Entries)
    fh, err := read_write.OpenFile("test.bin")
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
    fmt.Print("TB1: ")
    fmt.Println(tb1)
    fmt.Print("Read TB2: ")
    fmt.Println(tb2)
    entries.AddEntry(&tb1, int32(1172837485), "EdosWhooo")
    entries.AddEntry(&tb1, int32(10), "Delcos")
    fmt.Println(tb1.Entries.Values)
    err = entries.ReadEntry(tb1, 0)
    if err != nil {
        fmt.Println(err)
    }
}

