package dbms

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/types"
)

func AddTableToDatabase (db *types.Database_t, tbName string, cols []types.Column_t) error {
    for _, table := range db.Tables {
        if table.Name == tbName {
            return errors.New("Invalid table name")
        }
    }
    newTb := types.Table_t{
        NumOfColumns: uint32(len(cols)),
        Name: tbName,
        Columns: cols,
    }
    db.Tables = append(db.Tables, &newTb)
    db.NumOfTables = uint16(len(db.Tables))
    return nil
}



func WriteDatabase (db *types.Database_t) error {
    fileName := db.Name + ".selia"
    f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }

    if db.NumOfTables != uint16(len(db.Tables)){
        return errors.New("Table length does not match tables")
    }
    err = binary.Write(f, binary.LittleEndian, db.NumOfTables)
    if err != nil {
        return err
    }
    
    _, err = f.Write([]byte(db.Name+"\000"))
    if err != nil {
        return err
    }
    
    for _, table := range db.Tables {
        path := table.Name+".tb"
        err = entries.WriteTableToFile(table, path)
        if err != nil {
            return err
        }
        fmt.Println(table)
        _, err = f.Write([]byte(table.Name+"\000"))
        if err != nil {
            return err
        }
    }
    return nil
}



func ReadDatabase (dbName string) (types.Database_t, error) {
    fileName := dbName + ".selia"
    f, err := os.Open(fileName)
    if err != nil {
        return types.Database_t{}, err
    }
    newDB := types.Database_t{}
    tables := make([]*types.Table_t, 0)
    newDB.Tables = tables
    
    err = binary.Read(f, binary.LittleEndian, &newDB.NumOfTables)
    if err != nil {
        return types.Database_t{}, err
    }

    newDBName, err := entries.ReadStringFromFile(f, types.MAX_COLUMN_NAME_LENGTH)
    if err != nil {
        return types.Database_t{}, err
    }

    if dbName != string(newDBName){
        return types.Database_t{}, errors.New(fmt.Sprintf("DBnames don't match: got %s, expected %s", dbName, string(newDBName)))
    }

    newDB.Name = string(newDBName)
    
    for range newDB.NumOfTables {
        buffer, err := entries.ReadStringFromFile(f, types.MAX_TABLE_NAME_LENGTH)
        if err != nil {
            return types.Database_t{}, err
        }
        fmt.Println("Read this tablename", string(buffer))
        fh := entries.FileHandler{
            Path: string(buffer)+".tb",
        }
        newTB := types.Table_t{}
        err = fh.ReadTableFromFile(&newTB, 0)
        if err != nil {
            return types.Database_t{}, err
        }
        newDB.Tables = append(newDB.Tables, &newTB)
    }
    return newDB, nil
}



