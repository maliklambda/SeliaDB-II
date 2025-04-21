package main

import (
	"fmt"

	// "github.com/MalikL2005/Go_DB/btree"
	"github.com/MalikL2005/Go_DB/btree"
	"github.com/MalikL2005/Go_DB/entries"
	"github.com/MalikL2005/Go_DB/search"
	"github.com/MalikL2005/Go_DB/types"
)


func main (){
    col1 := types.Column_t {
        Name: "id",
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
        Columns: []types.Column_t{col1, col2, col3},
    }
    fmt.Println(len(tb1.Columns))
    fmt.Println(tb1.Columns)
    fmt.Println(tb1.Entries)
    fh, err := entries.OpenFile("test.bin")
    if err != nil {
        fmt.Println(err)
        panic(1)
    }

    err = fh.WriteTableToFile(&tb1, 0)
    if err != nil {
        fmt.Println(err)
        panic(1)
    }
    err = entries.AddEntry(&tb1, fh, int32(100), "EdosWhooo", "edos@gmail.com")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    err = entries.AddEntry(&tb1, fh, int32(44), "Delcos", "delcos_2201@gmx.de")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    err = entries.AddEntry(&tb1, fh, int32(51), "Wuschlee", "wuschlee-lorencius@mail.de")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    err = entries.AddEntry(&tb1, fh, int32(112), "DadanCheng", "Dadan-cheng@mail.de")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    err = entries.AddEntry(&tb1, fh, int32(51), "Nafu", "Nagyi-Fufu@lost.sk")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    _, err = entries.ReadEntryIndex(tb1, 1)
    if err != nil {
        fmt.Println(err)
    }
    tb2 := types.Table_t{}
    fh.ReadTableFromFile(&tb2, 0)
    fmt.Print("TB1: ")
    fmt.Println(tb1)
    fmt.Print("Read TB2: ")
    fmt.Println(tb2)
    entries.UpdateOffsetLastEntry(fh, 0, 5000)
    fh.ReadTableFromFile(&tb2, 0)
    fmt.Print("Read TB2: ")
    fmt.Println(tb2)
    search.IterateOverEntries(tb1)
    entry, err := search.FindEntryByKey(tb1, "id", 44)
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println(entry)
    btree.Traverse(*fh.Root, *fh.Root)
    entr := btree.SearchKey(fh.Root, *fh.Root, uint32(112))
    if entr == nil {
        fmt.Println("Error")
        return
    }
    fmt.Println(*entr)
    values, err := entries.ReadEntryFromFile(&tb1, int(entr.Value), &fh)
    if err != nil {
        fmt.Println("Error", err)
        return
    }
    fmt.Println(values)
}

