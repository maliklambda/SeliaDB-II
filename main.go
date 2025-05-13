package main

import (
	"fmt"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/search"
	"github.com/MalikL2005/SeliaDB-II/dbms"
	"github.com/MalikL2005/SeliaDB-II/types"
)


func main (){
    col1 := types.Column_t {
        Name: "id",
        Type: types.INT32,
        Size: 4,
    }
    col2 := types.Column_t {
        Name: "name",
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
    entries.AddEntry(&tb1, &fh, int32(25), "Dadi", "dadan.cheng@woo-mail.de")
    btree.Traverse(*fh.Root, *fh.Root)
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
    entr = btree.SearchKey(fh.Root, *fh.Root, uint32(23))
    fmt.Println("here", entr)
    entries.ReadEntryFromFile(&tb1, int(entr.Value), &fh)
    if err = dbms.AddColumn(&fh, &tb1, "age", "INT32", 0, int32(10)); err != nil {
        fmt.Println(err)
        return
    }
    search.IterateOverEntriesInFile(&fh, &tb1)

    entries_bt, err := search.FindEntryWhereCondition(&fh, &tb1, types.CompareObj{ColName: "age", CmpOperator: types.GREATER_EQUAL, Value: int32(10)}, 5)
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println(entries_bt)

    etriesFiltered := btree.TraverseWithFilter(*fh.Root, *fh.Root, &([]btree.Entry_t{}), btree.CompareBtreeKeys, types.SMALLER, uint32(200), btree.PrintEntry)
    fmt.Println(etriesFiltered)


    // err = dbms.DeleteColumn(&tb1, &fh, "age")
    // if err != nil {
    //     fmt.Println(err)
    //     return
    // }
    fmt.Println(tb1)
}

