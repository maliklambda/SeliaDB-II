package entries

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/types"
)


func InsertToBtree (root **btree.Node_t, key any, val uint32, tp types.Type_t) error {
    if root == nil {
        newRoot := &btree.Node_t{}
        root = &newRoot
    }
    err := btree.Insert(root, *root, btree.Entry_t{Key:key, Value:val}, tp)
    if err != nil {
        return err
    }
    btree.Traverse(*root, *root)
    return nil
}



func AddIndex(tb *types.Table_t, colName string) error {
    // colName does not exist
    colIndex, err := FindColNameIndex(tb, colName)
    if err != nil {
        return err
    }
    newRoot := &btree.Node_t{}
    fmt.Println("\n\n\n\nIndexing", colName, "of type", tb.Columns[colIndex].Type)
    currentPos := tb.StartEntries
    for range tb.Entries.NumOfEntries {
        fmt.Println("Reading entry at", currentPos)
        buffer, err := ReadEntryFromFile(tb, int(currentPos))
        if err != nil {
            return err
        }

        fmt.Println("inserting to index", buffer[colIndex])

        // if buffer[colIndex] is nil { return error }
        if len(buffer) <= colIndex {
            return errors.New("Problem with index")
        }
        switch(tb.Columns[colIndex].Type){
        // case types.FLOAT32:
        // err = InsertToBtree(&newRoot, float32(binary.LittleEndian.(buffer[colIndex])), uint32(currentPos), tb.Columns[colIndex].Type)
        case types.INT32:
            err = InsertToBtree(&newRoot, int32(binary.LittleEndian.Uint32(buffer[colIndex])), uint32(currentPos), tb.Columns[colIndex].Type)
        case types.VARCHAR:
            err = InsertToBtree(&newRoot, string(buffer[colIndex]), uint32(currentPos), tb.Columns[colIndex].Type)
        }
        if err != nil {
            return err
        }
        currentPos += uint16(GetEntryLength(buffer))
    }
    // tb.Indeces = append(tb.Indeces, types.Index_t{Root: (*any)(newRoot)})

    fmt.Println(newRoot)
    btree.Traverse(newRoot, newRoot)
    fmt.Println("Sucessfully indexed ", colName)
    tb.Indeces = append(tb.Indeces, types.Index_t{
        ColIndex: uint32(colIndex),
        // Root: (*any)(unsafe.Pointer(newRoot)),
        Root: btree.UnsafePNode_tToPAny(newRoot),
    })
    return nil
}



func FindColNameIndex (tb * types.Table_t, colName string) (int, error) {
    for i, col := range tb.Columns {
        if strings.ToUpper(col.Name) == strings.ToUpper(colName) {
            fmt.Println("Found colname in tb.columns")
            return i, nil
        }
    }
    return -1, errors.New(fmt.Sprintf("Column %s does not exist in table", colName))
}



