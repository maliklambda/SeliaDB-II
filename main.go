package main

import (
	"fmt"

	"github.com/MalikL2005/Go_DB/btree"
	"github.com/MalikL2005/Go_DB/entries"
	"github.com/MalikL2005/Go_DB/search"
	"github.com/MalikL2005/Go_DB/dbms"
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

    fh, err := entries.CreateFile("tb1.tb")
    if err != nil {
        fmt.Println("Error opening fh")
        return
    }
    entries.WriteTableToFile(&tb1, &fh)

    entries.AddEntry(&tb1, &fh, int32(23), "EdosWhoo", "Edos@gmail.com")
    entries.AddEntry(&tb1, &fh, int32(24), "Delcos", "Delcos2201@gmail.com")
    entries.AddEntry(&tb1, &fh, int32(22), "WuschLee", "WuschLee-Lorencius@mail.de")
    entry, err := search.FindEntryByKey(&tb1, "email", "EdosW@gmail.com")
    entr := btree.SearchKey(fh.Root, *fh.Root, uint32(22))
    fmt.Println(entr)
    entries.ReadEntryFromFile(&tb1, int(entr.Value), &fh)

    fmt.Println(db1)
    fmt.Println(entry)
    
    if err = fh.ReadTableFromFile(&tb2, 0); err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println("Before:", tb2)
    if err = dbms.AddColumn(&fh, &tb1, "age", "INT32", 0); err != nil {
        fmt.Println(err)
        return
    }
    search.IterateOverEntriesInFile(&fh, &tb1)

}

