package entries

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
  "os"

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
    if tb.Entries == nil || tb.Entries.NumOfEntries == 0 {
        tb.Indeces = append(tb.Indeces, types.Index_t{
            ColIndex: uint32(colIndex),
            Root: btree.UnsafePNode_tToPAny(newRoot),
        })
        return nil
    }
		fmt.Println(tb.Entries.NumOfEntries)
    for {
        fmt.Println("Reading entry at", currentPos)
        buffer, pNextEntry, err := ReadEntryFromFile(tb, int(currentPos))
        if err != nil {
						break
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
            err = InsertToBtree(&newRoot, int32(binary.LittleEndian.Uint32(buffer[colIndex])), uint32(currentPos)- uint32(tb.StartEntries), tb.Columns[colIndex].Type)
        case types.VARCHAR:
            err = InsertToBtree(&newRoot, string(buffer[colIndex]), uint32(currentPos) - uint32(tb.StartEntries), tb.Columns[colIndex].Type)
        }
        if err != nil {
            return err
        }
        currentPos = uint16(pNextEntry)
    }
    tb.Indeces = append(tb.Indeces, types.Index_t{Root: btree.UnsafePNode_tToPAny(newRoot)})

    fmt.Println(newRoot)
    btree.Traverse(newRoot, newRoot)
    fmt.Println("Sucessfully indexed ", colName)
    fmt.Println(colIndex)

    err = UpdateIsColIndexed(tb, colIndex)
    if err != nil {
        return err
    }
    
    tb.Indeces = append(tb.Indeces, types.Index_t{
        ColIndex: uint32(colIndex),
        Root: btree.UnsafePNode_tToPAny(newRoot),
    })
    return nil
}



func FindColNameIndex (tb * types.Table_t, colName string) (int, error) {
    for i, col := range tb.Columns {
				if strings.EqualFold(col.Name, colName){
            fmt.Println("Found colname in tb.columns")
            return i, nil
        }
    }
    return -1, fmt.Errorf("Column %s does not exist in table %s", colName, tb.Name)
}


func UpdateIsColIndexed (tb * types.Table_t, colIndex int) error {
    if colIndex >= len(tb.Columns){
        return fmt.Errorf("Column index is too large: got %d but have only %d column(s)", colIndex, len(tb.Columns))
    }
    tb.Columns[colIndex].Indexed = true
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    bufferCol := types.Column_t{}
    var offset int64

    // Read table
    startColumns := binary.Size(tb.NumOfColumns) + len([]byte(tb.Name+"\000")) + binary.Size(tb.StartEntries) + binary.Size(tb.EndOfTableData)
    _, err = f.Seek(int64(startColumns), 0)
    if err != nil {
        return err
    }
    // read columns
    fmt.Println(tb.NumOfColumns)
    for i := range (colIndex+1) {
        fmt.Println("iterating", i)
        offset, err = f.Seek(0, 1)
        if err != nil {
            return err
        }
        bufferCol, err = ReadColumnFromFile(f, offset)
        if err != nil {
            return err
        }
    }
    fmt.Println("updating on column:", bufferCol)
    bufferCol.Indexed = true
    _, err = f.Seek(offset, 0)
    if err != nil {
        return err
    }

    // write changes to file
    err = WriteColumnToFile(bufferCol, offset, f)
    if err != nil {
        return err
    }

    return nil
}



