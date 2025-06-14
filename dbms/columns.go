package dbms

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	// "reflect"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/entries"
	"github.com/MalikL2005/SeliaDB-II/types"
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
    err = insertColumnToFile(tb, newCol)
    if err != nil {
        fmt.Println("ererrrrrr\n\n\n")
        return err
    }
    if err = entries.UpdateNumOfColumns(tb, tb.NumOfColumns+1); err != nil {
        return err
    }

    // fmt.Println("\n\nstart entries:", tb.StartEntries)
    // colSize := uint16(newCol.GetColSize())
    // fmt.Println("offset :", colSize)
    // if err = entries.UpdateStartEntries(tb, tb.StartEntries+uint16(types.GetTableDataBuffer())); err != nil {
    //     return err
    // }

    fmt.Println("New Start entries", tb.StartEntries)

    // defaultValueAsType := reflect.ValueOf(defaultValue)
    // isDefaultValue := !defaultValueAsType.IsZero()
    //
    // if isDefaultValue {       
    //     // iterate over all entries, insert defaultValue for column 
    //     fmt.Println(defaultValue)
    //     err = insertDefaultValue(tb, newCol, defaultValue)
    //     if err != nil {
    //         return err
    //     }
    //     tb.Columns = append(tb.Columns, newCol)
    //     return nil
    // }
    
    // iterate over all entries, insert null for column 
    // currentPos := tb.StartEntries + uint16(types.GetEntryBuffer())
    // values := [][][]byte{}
    // for range tb.Entries.NumOfEntries {
    //     fmt.Println("Reading entry at", currentPos)
    //     buffer, pNextEntry, err := entries.ReadEntryFromFile(tb, int(currentPos))
    //     if err != nil {
    //         return err
    //     }
    //     values = append(values, buffer)
    //     currentPos += uint16(entries.GetEntryLength(buffer))
    //     fmt.Println("\n\n\nAllocating", newCol.Size, "Bytes at", currentPos)
    //     _, err = appendNullValuesToFile(tb, &newCol, int64(currentPos))
    //     if err != nil {
    //         return err
    //     }
    //     currentPos = uint16(pNextEntry)
    // }
    // append null values to end of file
    // This is necessary because method AllocateInFile() returns EOF for the last value
    // f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    // if err != nil {
    //     return err
    // }
    // defer f.Close()
    // _, err = f.Seek(0, 2)
    // if err != nil {
    //     return err
    // }
    // if newCol.Type == types.VARCHAR {
    //     _, err = f.Write([]byte("\000"))
    //     if err != nil {
    //         return err
    //     }
    // } else {
    //     nullBytes := make([]byte, colSize)
    //     _, err = f.Write(nullBytes)
    //     if err != nil {
    //         return err
    //     }
    // }
    //
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
    if col.GetColSize() >= int(tb.StartEntries) - int(tb.EndOfTableData){
        fmt.Println("allocating more space")
        if err := types.AllocateInFile(tb.MetaData.FilePath, int64(tb.EndOfTableData), int64(binary.Size(col))); err != nil {
            return err
        }
    }

    fmt.Println("writing column @", tb.EndOfTableData)
    fmt.Println(col.GetColSize())
    f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()
    err = entries.WriteColumnToFile(col, int64(tb.EndOfTableData), f)
    if err != nil {
        return err
    }

    // err = entries.UpdateEndOfTableData(tb, tb.EndOfTableData+ uint16(binary.Size(*col)))
    // if err != nil {
    //     return err
    // }

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





func insertDefaultValue(tb *types.Table_t, newCol types.Column_t, defaultValue any) error {
    insertSize := newCol.Size
    if newCol.Type == types.VARCHAR {
        s, ok := defaultValue.(string)
        if !ok {
            return errors.New("Expected type to be varchar. defaultvalue does not match")
        }
        insertSize = uint16(len(s))+1
    }
    currentPos := tb.StartEntries
    values := [][][]byte{}
    for range tb.Entries.NumOfEntries {
        fmt.Println("Reading entry at", currentPos)
        buffer, pNextEntry, err := entries.ReadEntryFromFile(tb, int(currentPos))
        if err != nil {
            return err
        }
        values = append(values, buffer)
        currentPos += uint16(entries.GetEntryLength(buffer))
        fmt.Println("\n\n\nWriting", insertSize, "Bytes at", currentPos)
        fmt.Println("Writing", defaultValue)
        _, err = writeInFile(tb, int64(currentPos), int64(insertSize), defaultValue, newCol.Type)
        if err != nil {
            return err
        }
        currentPos = uint16(pNextEntry)
    }

    // write default to EOF 
    err := writeToEOF(tb, defaultValue, newCol.Type)
    if err != nil {
        return err
    }

    return nil
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
    fmt.Println(*tb.Entries)
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
    entries.DeleteBytesFromTo(tb.MetaData.FilePath, startOffset, startOffset+int64(len(colName)+1))

    // update NumOfColumns
    err = entries.UpdateNumOfColumns(tb, tb.NumOfColumns-1)
    if err != nil {
        return err
    }

    // update StartEntries

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

    // return 0, nil
}
