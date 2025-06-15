package dbms

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	// "reflect"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/types"
	"github.com/MalikL2005/SeliaDB-II/entries"
)

func AddColumn (tb *types.Table_t, colName, colType string, varCharLen uint32, isIndexed bool, defaultValue any) error {
    tp, err := types.StringToType_t(colType)
    if err != nil {
        return err
    }
    fmt.Println("\n\n\n\ntype", tp)
    size, err := tp.GetTypeSize(varCharLen)
    if err != nil {
        return err
    }

    fmt.Println(tp, "size", size)
    if entries.ExistsColumnName(tb, colName){
        return errors.New("Column name already exists")
    }
    newCol := types.Column_t{
        Name: colName,
        Type: tp,
        Size: uint16(size),
        Indexed: isIndexed,
    }
    fmt.Println("New column:", newCol)
    if err = insertColumnToFile(tb, newCol); err != nil {
        fmt.Println("ererrrrrr\n\n\n")
        return err
    }
    if err = entries.UpdateNumOfColumns(tb, tb.NumOfColumns+1); err != nil {
        return err
    }



    fmt.Println("New Start entries", tb.StartEntries)

    // btreeOffsetUpdate := make(map[int]int32)
    // append either null values or default value
    currentPos := tb.StartEntries
    for {
        val, pNextEntry, err := entries.ReadEntryFromFile(tb, int(currentPos))
        if err != nil {
            fmt.Println("exiting\n\n ")
            break
        }
        entryLength := entries.GetEntryLength(val)
        // if int64(binary.Size(defaultValue)) >= pNextEntry {
        //     fmt.Println("Buffer is full")
        //     err = types.AllocateInFile(tb.MetaData.FilePath, int64(currentPos)+int64(entryLength)+int64(binary.Size(types.PNextEntrySize)), int64(types.GetEntryBuffer()))
        //     if err != nil {
        //         return err
        //     }
        //     pNextEntry += int64(types.GetEntryBuffer())
        //     // update btree values
        // }

        // write defaultValue
        f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            return err
        }
        if _, err = f.Seek(int64(currentPos)+int64(entryLength), 0); err != nil {
            return err
        }

        fmt.Println("writing defaultValue @", int(currentPos)+entryLength)
        if _, err = f.Write([]byte(defaultValue.(string)+"\000")); err != nil {
            return err
        }

        pos, _ := f.Seek(0, 1)
        fmt.Println("writing pNextEntry", pNextEntry, "@", pos)
        if err = binary.Write(f, binary.LittleEndian, uint16(42)); err != nil {
            return err
        }
        f.Close()
        // tb.Columns = append(tb.Columns, newCol) // delete this later!
        // e, ne, _ := entries.ReadEntryFromFile(tb, int(currentPos))
        // fmt.Println("\n\n\n\nREad this entry", e)
        // fmt.Println("Next one @", ne)

        // break
        currentPos = uint16(pNextEntry)
    }
    

    fmt.Println("this was a success (?)\n\n ")


    tb.Columns = append(tb.Columns, newCol)
    return nil
}



// Returns number of bytes written and error
func appendNullValuesToFile (tb *types.Table_t, col *types.Column_t, currentPos int64) (int, error) {
    if col.Type == types.VARCHAR {
        err := types.AllocateInFile(tb.MetaData.FilePath, int64(currentPos), int64(1))
        if err != nil {
            return 0, err
        }
        return 1, nil
    }
    err := types.AllocateInFile(tb.MetaData.FilePath, int64(currentPos), int64(col.Size))
    if err != nil {
        return 0, err
    }
    return int(col.Size), nil
}



func insertColumnToFile (tb *types.Table_t, col types.Column_t) error {
    fmt.Println("writing column @", tb.EndOfTableData)
    fmt.Println(col.GetColSize())

    if tb.EndOfTableData + uint16(col.GetColSize()) >= tb.StartEntries {
        fmt.Println("allocating more space")
        if err := types.AllocateInFile(tb.MetaData.FilePath, int64(tb.EndOfTableData), int64(types.GetTableDataBuffer())); err != nil {
            return err
        }
        fmt.Println("updating start entries ")
        if err := entries.UpdateStartEntries(tb, tb.StartEntries+uint16(types.GetTableDataBuffer())); err != nil {
            return err
        }
    } else {
        fmt.Println("eof-data:", tb.EndOfTableData, "- colSize:", col.GetColSize(), "- start entries:", tb.StartEntries)
    }

    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    if err = entries.WriteColumnToFile(col, int64(tb.EndOfTableData), f); err != nil {
        return err
    }

    if err = entries.UpdateEndOfTableData(tb, tb.EndOfTableData+ uint16(binary.Size(col))); err != nil {
        return err
    }
    return nil
}




func WriteColumnAtOffset (tb *types.Table_t, col *types.Column_t, offset int64) error {
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    if _, err := f.Seek(offset, 0); err != nil {
        return err
    }

    if _, err = f.Write([]byte(col.Name+"\000")); err != nil {
        return err
    }

    if err = binary.Write(f, binary.LittleEndian, col.Type); err != nil {
        return err
    }

    if err = binary.Write(f, binary.LittleEndian, col.Size); err != nil {
        return err
    }

    return nil
}



func moveBtreeEntries (root *btree.Node_t, current *btree.Node_t, entryList *[]btree.Entry_t, colSize int, colTypeSize int) error {
    fmt.Println("Moving btree entries")
    *entryList = createEntryListSortedByOffset(root, current, entryList)
    fmt.Println(entryList)
    err := updateBtreeValues(root, current, entryList, colSize, colTypeSize)
    if err != nil {
        return err
    }
    return nil
}


func updateBtreeValues(root *btree.Node_t, current *btree.Node_t, entryList*[]btree.Entry_t, colSize int, colTypeSize int) error {
    if current == nil {
        return nil
    }

    for i, entry := range *current.Entries {
        index := findIndex(*entryList, entry.Key)
        if index < 0 {
            return errors.New("entryList is not complete")
        }
        (*current.Entries)[i].Value = uint32(int(entry.Value) + colSize + (colTypeSize * index))
        fmt.Println("\nNew", (*current.Entries)[i])
    }

    if current.Children == nil {
        return nil
    }
    for _, child := range *current.Children {
        updateBtreeValues(root, &child, entryList, colSize, colTypeSize)
    }
    return nil
}


func createEntryListSortedByOffset(root *btree.Node_t, current *btree.Node_t, entryList *[]btree.Entry_t) []btree.Entry_t {
    if current == nil {
        return *entryList
    }

    for _, entry := range *current.Entries {
        // insert in ordered fashion
        *entryList = insertToSliceSortedByOffset(*entryList, entry)
    }

    if current.Children == nil {
        return *entryList
    }
    for _, child := range *current.Children {
        createEntryListSortedByOffset(root, &child, entryList)
    }
    return *entryList
}


func insertToSliceSortedByOffset (arr []btree.Entry_t, value btree.Entry_t) []btree.Entry_t {
    for i, entry := range arr {
        if entry.Value > value.Value {
            arr = append(arr, btree.Entry_t{})
            copy(arr[i+1:], arr[i:])
            arr[i] = value
            return arr
        }
    }
    arr = append(arr, value)
    return arr
}

// returns -1 if not found
func findIndex (arr []btree.Entry_t, key any) int {
    for i, entry := range arr {
        if entry.Key == key {
            return i
        }
    }
    return -1
}




func writeInFile(tb *types.Table_t, offset int64, numBytes int64, defaultValue any, dvType types.Type_t) (int, error){
    err := types.AllocateInFile(tb.MetaData.FilePath, offset, numBytes)
    if err != nil {
        return 0, err
    }

    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return 0, err
    }
    defer f.Close()

    _, err = f.Seek(offset, 0)
    if err != nil {
        return 0, err
    }
    
    if dvType == types.VARCHAR {
        fmt.Println("\n\nDFSize VARCHAR\n\n\n", len(string(defaultValue.(string))))
        s, ok := defaultValue.(string)
        if !ok {
            return 0, errors.New("Expected varchar type. Type does not match")
        }

        _, err = f.Write([]byte(s+"\000"))
        if err != nil {
            return 0, err
        }

        return len(s)+1, nil
    }

    err = binary.Write(f, binary.LittleEndian, defaultValue)
    if err != nil {
        return 0, err
    }
    fmt.Println("\n\n\nSuccessfully written", defaultValue, "to file")

    f.Seek(offset, 0)
    bt := make([]byte, 4)
    f.Read(bt)
    fmt.Println("Read this from file:", bt)

    return int(numBytes), nil
}



func writeToEOF (tb *types.Table_t, defaultValue any, tp types.Type_t) error {
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    _, err = f.Seek(0, 2)
    if err != nil {
        return err
    }

    if tp == types.VARCHAR {
        _, err = f.Write([]byte(defaultValue.(string)))
        if err != nil {
            return err
        }
        return nil
    }

    err = binary.Write(f, binary.LittleEndian, defaultValue)
    if err != nil {
        return err
    }

    return nil
}



func DeleteColumn (tb * types.Table_t, colName string) error {
    // check if colName is in tb.Columns
    index, err := entries.FindColNameIndex(tb, colName)
    if err != nil {
        return err
    }

    // find colName in file
    startOffset, err := findColNameInFile (tb, colName, int64(index))
    if err != nil {
        return err
    }

    // delete colName from file
    if err = entries.DeleteBytesFromTo(tb.MetaData.FilePath, startOffset, startOffset+int64(len(colName)+1)); err != nil {
        return err
    }

    // update start entries

    // update NumOfColumns
    err = entries.UpdateNumOfColumns(tb, tb.NumOfColumns-1)
    if err != nil {
        return err
    }


    // update btree offsets
    
    return nil
}


// Finds the name of a column in file (index is the index of ColName in the table in memory)
// Returns offset to the start of the colName string
func findColNameInFile (tb *types.Table_t, colName string, index int64) (int64, error){
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return 0, err
    }
    defer f.Close()

    // move to first column
    offsetToFirstCol, err := types.GetOffsetToFirstColumn(tb)
    if err != nil {
        return 0, err
    }

    _, err = f.Seek(offsetToFirstCol, 0)
    if err != nil {
        return 0, err
    }

    var bufferCol types.Column_t
    for range index {
        pos, _ := f.Seek(0, 1)
        fmt.Println("current pos:", pos)
        bufferCol, err = entries.ReadColumnFromFile(f, pos)
        if err != nil {
            return 0, err
        }
        fmt.Println("current buffer:", bufferCol)
    }

    pos, _ := f.Seek(0, 1)
    if string(bufferCol.Name) == colName {
        fmt.Println("Must delete column at this offset:", pos)
        expectedLength := bufferCol.GetColSize()
        fmt.Println("Deleting", expectedLength, "bytes from file")
        buf := make([]byte, expectedLength)
        actual, err := f.Read(buf)
        if actual != expectedLength {
            return 0, errors.New("Wff happened there?")
        }
        if err != nil {
            return 0, err
        }
        fmt.Println("Expecting", []byte(colName+"\000"), "to be the same as", buf)
        return pos, nil
    } else {
        fmt.Println("Buffer and colname dont match.")
        return 0, errors.New(fmt.Sprintf("Buffer (%s) and colname (%s) are expected to be the same", string(bufferCol.Name), colName))
    }

    return 0, nil
}



