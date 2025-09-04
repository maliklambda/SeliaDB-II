package process

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/MalikL2005/SeliaDB-II/commands/parser"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/types"
	"github.com/MalikL2005/SeliaDB-II/joins"
)


func INSERT (query string, db * types.Database_t) error {
    tbString, colValues, err := parser.ParseInsert(query, db)
    if err != nil {
        return err
    }

    tbIndex, err := joins.GetTableIndex(db, tbString)
    if err != nil {
        return err
    }
    tb := db.Tables[tbIndex]

    err = processInsertQuery(tb, *colValues)
    if err != nil {
        return err
    }

    return nil
}



func processInsertQuery(tb *types.Table_t, colValues map[string] string) error {
    values := make([]any, 0)
    for _, col := range tb.Columns {
        val, ok := colValues[col.Name]
        if !ok {
            return errors.New(fmt.Sprintf("Missing specification for column \"%s\"", col.Name))
        }
        switch col.Type {
            case types.VARCHAR:
                values = append(values, val)

            case types.INT32:
                valInt, err := strconv.ParseInt(val, 10, 32)
                if err != nil {
                    return err
                }
                values = append(values, int32(valInt))
            case types.BOOL:
                valBool, err := strconv.ParseBool(val)
                if err != nil {
                    return err
                }
                values = append(values, valBool)
            case types.FLOAT32:
                valFloat, err := strconv.ParseFloat(val, 32)
                if err != nil {
                    return err
                }
                values = append(values, valFloat)
        }
    }

    fmt.Println(values)
    if len(values) != int(tb.NumOfColumns) {
        return errors.New(fmt.Sprintf("Incorrect num of values: Expected %d, got: %d", tb.NumOfColumns, len(values)))
    }

    // btree entries are added in AddEntry()
    err := entries.AddEntry(tb, values...)
    if err != nil {
        return err
    }

    return nil
}


