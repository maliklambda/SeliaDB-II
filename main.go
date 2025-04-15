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
    col3 := types.Column_t {
        Name: "email",
        Type: types.VARCHAR,
        Size: 100,
    }

    tb1 := types.Table_t {
        Name: "tb1",
        NumOfColumns: 3,
        OffsetToLastEntry: 0,
        Columns: []types.Column_t{col1, col2, col3},
    }
    fmt.Println(len(tb1.Columns))
    fmt.Println(tb1.Columns)
    fmt.Println(tb1.Entries)
    fh, err := read_write.OpenFile("test.bin")
    if err != nil {
        fmt.Println(err)
        panic(1)
    }

    err = fh.WriteTableToFile(&tb1, 0)
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
    err = entries.AddEntry(&tb1, int32(1172837485), "EdosWhooo", "edos@gmail.com")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    err = entries.AddEntry(&tb1, int32(10), "Delcos", "delcos_2201@gmx.de")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    fmt.Println(tb1.Entries.Values)
    err = entries.ReadEntry(tb1, 0)
    if err != nil {
        fmt.Println(err)
    }
}

