package main

import (
	"fmt"

	"github.com/MalikL2005/SeliaDB-II/commands"
	"github.com/MalikL2005/SeliaDB-II/dbms"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/search"
	"github.com/MalikL2005/SeliaDB-II/types"
)


func main (){
    var err error

    col1 := types.Column_t {
        Name: "id",
        Type: types.INT32,
        Size: 4,
    }
    col2 := types.Column_t {
        Name: "name",
        Type: types.VARCHAR,
        Size: 255,
        Indexed: false,
    }
    col3 := types.Column_t {
        Name: "email",
        Type: types.VARCHAR,
        Size: 100,
    }

    col4 := types.Column_t {
        Name: "job",
        Type: types.VARCHAR,
        Size: 100,
    }

    tb1 := &types.Table_t {
        Name: "tb1",
        NumOfColumns: 3,
        Columns: []types.Column_t{col1, col2, col3},
        Indeces: []types.Index_t{},
        MetaData: types.TableMetaData_t{FilePath: "out/tb1.tb"},
    }
    if err = entries.WriteTableToFile(tb1); err != nil {
        fmt.Println(err)
        return
    }
    
    // tb2, err := entries.ReadTableFromFile(tb1.MetaData.FilePath)
    // if err != nil {
    //     fmt.Println(err)
    //     return
    // }
    // fmt.Println(tb2)
    tb2 := &types.Table_t {
        Name: "tb2",
        NumOfColumns: 2,
        Columns: []types.Column_t{col1, col3},
        Indeces: []types.Index_t{},
        MetaData: types.TableMetaData_t{FilePath: "out/tb2.tb"},
    }
    if err = entries.WriteTableToFile(tb2); err != nil {
        fmt.Println(err)
        return
    }
    

    tb3 := &types.Table_t {
        Name: "tb3",
        NumOfColumns: 3,
        Columns: []types.Column_t{col1, col2, col3},
        Indeces: []types.Index_t{},
        MetaData: types.TableMetaData_t{FilePath: "out/tb3.tb"},
    }
    if err = entries.WriteTableToFile(tb3); err != nil {
        fmt.Println(err)
        return
    }

    tb4 := &types.Table_t {
        Name: "tb4",
        NumOfColumns: 2,
        Columns: []types.Column_t{col1, col4},
        Indeces: []types.Index_t{},
        MetaData: types.TableMetaData_t{FilePath: "out/tb4.tb"},
    }
    if err = entries.WriteTableToFile(tb4); err != nil {
        fmt.Println(err)
        return
    }

    db1 := &types.Database_t{
        Name: "db1",
        Tables: []*types.Table_t{tb1, tb2, tb3, tb4},
        NumOfTables: 2,
    }

    entries.AddEntry(tb1, int32(23), "EdosWhoo",  "Edos@gmail.com")
    entries.AddEntry(tb1, int32(24), "Delcos",    "Delcos2201@gmail.com")
    entries.AddEntry(tb1, int32(22), "WuschLee",  "WuschLee-Lorencius@mail.de")
    entries.AddEntry(tb1, int32(25), "Dadi",      "dadan.cheng@woo-mail.de")

		vals, maxLengths, err := search.IterateOverEntriesInFile(tb1, []int{}, 100)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb1.Columns, maxLengths)

    query := "INSERT INTO tb3 VALUES (id = 25, name = 'Malik Lorenz', email = 'malik@mail.com');"
    err = commands.CommandWrapper(query, db1)
    if err != nil {
        fmt.Println(err)
        return
    }
    query = "INSERT INTO tb2 VALUES (id = 25, name = 'Other Malik', email = 'malik_20234204@mail.com');"
    err = commands.CommandWrapper(query, db1)
    if err != nil {
        fmt.Println(err)
        return
    }

    query = "INSERT INTO tb4 VALUES (id = 25, job = 'Software Engineer @Deutsche Telekom');"
    err = commands.CommandWrapper(query, db1)
    if err != nil {
        fmt.Println(err)
        return
    }

    err = entries.AddIndex(tb1, "id")
    if err != nil {
        fmt.Println(err)
        return
    }
    err = entries.AddIndex(tb2, "id")
    if err != nil {
        fmt.Println(err)
        return
    }
    err = entries.AddIndex(tb4, "id")
    if err != nil {
        fmt.Println(err)
        return
    }
    err = entries.AddIndex(tb3, "id")
    if err != nil {
        fmt.Println(err)
        return
    }

    query = "SELECT name AS delcos, id AS kennzeichner FROM tb3;" 
    err = commands.CommandWrapper(query, db1)
    if err != nil {
        fmt.Println(err)
        return
    }

    entries.AddEntry(tb3, int32(23), "OtherEdosWhoo",  "Edos@gmail.com")

		vals, maxLengths, err = search.IterateOverEntriesInFile(tb3, []int{}, 100)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb1.Columns, maxLengths)

    query = "SELECT id FROM tb3 JOIN tb1 ON tb3.id = tb1.id JOIN tb2 ON tb1.id = tb2.id JOIN tb4 ON id = id LIMIT 10;"
    err = commands.CommandWrapper(query, db1)
    if err != nil {
        fmt.Println(err)
    }
		return

    // query = "SELECT * FROM tb3 WHERE name = 'Malik Lorenz';"
    // err = commands.CommandWrapper(query, db1)
    // if err != nil {
    //     fmt.Println(err)
    //     return
    // }

    query = "SELECT * FROM tb3;"
    err = commands.CommandWrapper(query, db1)
    if err != nil {
        fmt.Println(err)
        return
    }



		return



    fmt.Println(tb1)
    // err = entries.AddIndex(tb1, "name")
    // if err != nil {
    //     fmt.Println(err)
    // }
    // err = entries.AddIndex(tb1, "id")
    // if err != nil {
    //     fmt.Println(err)
    //     return
    // }

    fmt.Println(tb1)
    entries.ReadEntryFromFile(tb1, int(tb1.StartEntries))
    err = dbms.AddColumn(tb1, "i_am_new_and_I_have_many_characters", "VARCHAR", 60, false, "default_that_is_very_long") // still issues with adding defaultvalues that need new buffer
    if err != nil {
        fmt.Println(err)
        return
    }

    vals, maxLengths, err = search.IterateOverEntriesInFile(tb1, []int{}, 100)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb1.Columns, maxLengths)

    if err = entries.AddEntry(tb1, int32(24), "Sejaa",  "selos@gunther-mail.com", "default here"); err != nil {
        fmt.Println(err)
        return
    }
    
    if err = entries.AddEntry(tb1, int32(27), "Naginka",  "nagyi@gunther-mail.com", "default here also"); err != nil {
        fmt.Println(err)
        return
    }
    
    vals, maxLengths, err = search.IterateOverEntriesInFile(tb1, []int{}, 10000)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb1.Columns, maxLengths)

    fmt.Println(tb1)

    tb2, err = entries.ReadTableFromFile(tb1.MetaData.FilePath)
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println(tb2)

    vals, maxLengths, err = search.IterateOverEntriesInFile(tb1, []int{}, 10000)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb2.Columns, maxLengths)
    fmt.Println(tb2)


    err = dbms.AddColumn(tb2, "NewCol-exciting", "VARCHAR", 200, false, "Hello")
    if err != nil {
        fmt.Println(err)
        return
    }

    tb2, err = entries.ReadTableFromFile(tb1.MetaData.FilePath)
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println(tb2)


    fmt.Println(tb2.Columns)

    vals, maxLengths, err = search.IterateOverEntriesInFile(tb2, []int{}, 10000)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb2.Columns, maxLengths)

    err = dbms.AddColumn(tb2, "test_add", "VARCHAR", 200, false, "i_like_programming_in_rust")
    if err != nil {
        fmt.Println(err)
        return
    }
    err = dbms.AddColumn(tb2, "age", "INT32", 0, false, int32(10))
    if err != nil {
        fmt.Println(err)
        return
    }
    vals, maxLengths, err = search.IterateOverEntriesInFile(tb2, []int{}, 10000)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb2.Columns, maxLengths)
    fmt.Println(tb2)


    tb2, err = entries.ReadTableFromFile(tb1.MetaData.FilePath)
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println(tb2)

    vals, maxLengths, err = search.IterateOverEntriesInFile(tb2, []int{}, 10000)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb2.Columns, maxLengths)

    cmp1 := types.CompareObj{
        ColName: "email",
        CmpOperator: types.ENDS_WITH,
        Value: ".com",
        Connector: types.AND,
    }
    if err = entries.UpdateEntriesWhere(tb2, cmp1, "age", int32(25)); err != nil {
        fmt.Println(err)
        return
    }

    vals, maxLengths, err = search.IterateOverEntriesInFile(tb2, []int{}, 10000)
    if err != nil {
        fmt.Println(err)
        return
    }
    types.DisplayByteSlice(vals, tb2.Columns, maxLengths)

    // db1 := &types.Database_t{
    //     Name: "db1",
    //     Tables: []*types.Table_t{tb2},
    //     NumOfTables: 1,
    // }
    //
    // query := "select * from tb1 where email='Edos@gmail.com';"
    // if err = commands.CommandWrapper(query, db1); err != nil {
    //     fmt.Println(err)
    //     return
    // }
    



}

