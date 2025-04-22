package main

import (
	"fmt"

	// "github.com/MalikL2005/Go_DB/btree"
	// "github.com/MalikL2005/Go_DB/btree"
	"github.com/MalikL2005/Go_DB/entries"
	"github.com/MalikL2005/Go_DB/search"
	"github.com/MalikL2005/Go_DB/types"
	// "github.com/MalikL2005/Go_DB/dbms"
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
    tb2 := types.Table_t {
        Name: "tb2",
        NumOfColumns: 2,
        Columns: []types.Column_t{col1, col3},
    }
    tbs := []*types.Table_t{&tb1, &tb2}
    db1 := types.Database_t{
        Name: "DBTEST",
        NumOfTables: 2,
        Tables: tbs,
    }
    // fmt.Println(len(tb1.Columns))
    // fmt.Println(tb1.Columns)
    // fmt.Println(tb1.Entries)
    // fh, err := entries.OpenFile("test.bin")
    // if err != nil {
    //     fmt.Println(err)
    //     panic(1)
    // }
    //
    // err = entries.WriteTableToFile(&tb1, fh, 0)
    // if err != nil {
    //     fmt.Println(err)
    //     panic(1)
    // }
    // err = entries.AddEntry(&tb1, fh, int32(44), "Delcos", "delcos_2201@gmx.de")
    // if err != nil {
    //     fmt.Println("Could not add entry", err)
    // }
    // err = entries.AddEntry(&tb1, fh, int32(51), "Wuschlee", "wuschlee-lorencius@mail.de")
    // if err != nil {
    //     fmt.Println("Could not add entry", err)
    // }
    // err = entries.AddEntry(&tb1, fh, int32(112), "DadanCheng", "Dadan-cheng@mail.de")
    // if err != nil {
    //     fmt.Println("Could not add entry", err)
    // }
    // err = entries.AddEntry(&tb1, fh, int32(51), "Nafu", "Nagyi-Fufu@lost.sk")
    // if err != nil {
    //     fmt.Println("Could not add entry", err)
    // }
    // _, err = entries.ReadEntryIndex(tb1, 1)
    // if err != nil {
    //     fmt.Println(err)
    // }
    // tb2 := types.Table_t{}
    // fh.ReadTableFromFile(&tb2, 0)
    // fmt.Print("TB1: ")
    // fmt.Println(tb1)
    // fmt.Print("Read TB2: ")
    // fmt.Println(tb2)
    // // entries.UpdateOffsetLastEntry(fh, 0, 5000)
    // fh.ReadTableFromFile(&tb2, 0)
    // fmt.Print("Read TB2: ")
    // fmt.Println(tb2)
    // search.IterateOverEntries(tb1)
    // entry, err := search.FindEntryByKey(tb1, "id", 44)
    // if err != nil {
    //     fmt.Println(err)
    //     return
    // }
    // fmt.Println(entry)
    // btree.Traverse(*fh.Root, *fh.Root)
    // entr := btree.SearchKey(fh.Root, *fh.Root, uint32(112))
    // if entr == nil {
    //     fmt.Println("Error")
    //     return
    // }
    // fmt.Println(*entr)
    // values, err := entries.ReadEntryFromFile(&tb1, int(entr.Value), &fh)
    // if err != nil {
    //     fmt.Println("Error", err)
    //     return
    // }
    // fmt.Println(values)

    fh, err := entries.CreateFile("tb1.tb")
    if err != nil {
        fmt.Println("Error opening fh")
        return
    }
    entries.WriteTableToFile(&tb1, fh)
    err = entries.AddEntry(&tb1, fh, int32(100), "EdosWhooo", "edos@gmail.com")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    err = entries.AddEntry(&tb1, fh, int32(50), "Delcos", "delcos2201@gmail.com")
    if err != nil {
        fmt.Println("Could not add entry", err)
    }
    fmt.Println()
    fmt.Println()
    fmt.Println(tb1)
    
    fmt.Println("Entries", db1.Tables[0].Entries)
    _, err = entries.ReadEntryIndex(*db1.Tables[0], 0)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(db1)

    // err = dbms.WriteDatabase(&db1)
    // if err != nil {
    //     fmt.Println("Error writing db1 to file:", err)
    //     return
    // }
    //
    // fmt.Println(db1)
    // var tbTest types.Table_t
    // fh.ReadTableFromFile(&tbTest, 0)
    // fmt.Println(tbTest)
    //
    // db2, err := dbms.ReadDatabase("DBTEST")
    // if err != nil {
    //     fmt.Println("Error reading db:", err)
    //     return
    // }
    //
    // fmt.Println(db2)
    // fmt.Println(db2.Tables[0])
    //
    err = search.IterateOverEntriesInFile(fh, &tb1)
    if err != nil {
        fmt.Println("Error iterating over entries on file", err)
        return
    }
}

