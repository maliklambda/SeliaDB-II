package dbms

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/MalikL2005/Go_DB/btree"
	"github.com/MalikL2005/Go_DB/entries"
	"github.com/MalikL2005/Go_DB/types"
)

func AddColumn (fh *entries.FileHandler, tb *types.Table_t, colName string, colType string, varCharLen uint32, defaultValue any) error {
    tp, err := types.StringToType_t(colType)
    if err != nil {
        return err
    }
    size, err := tp.GetTypeSize(varCharLen)
    if err != nil {
        return err
    }

    fmt.Println(tp, "size", size)
    fmt.Println(tb.StartEntries)
    if existsColumnName(tb, colName){
        return errors.New("Column name already exists")
    }
    newCol := types.Column_t{
        Name: colName,
        Type: tp,
        Size: size,
    }
    fmt.Println("New column:", newCol)
    insertColumnToFile(fh, tb, &newCol)
    if err = entries.UpdateNumOfColumns(fh, tb.NumOfColumns+1); err != nil {
        return err
    }

    fmt.Println("\n\nstart entries:", tb.StartEntries)
    colSize := uint16(newCol.GetColSize())
    fmt.Println("offset :", colSize)
    if err = entries.UpdateStartEntries(fh, tb.StartEntries+colSize); err != nil {
        return err
    }
    tb.StartEntries += colSize

    fmt.Println("New Start entries", tb.StartEntries)

    defaultValueAsType := reflect.ValueOf(defaultValue)
    isDefaultValue := !defaultValueAsType.IsZero()

    btreeMoveSize := int(newCol.Size) // necessary because varchar might change this from newCol.Size to defaultvalue + \0
    // check if default value is varchar -> update btreeMoveSize
    if isDefaultValue && tp == types.VARCHAR {
        fmt.Println("defaultvalue is varchar")
        s, ok := defaultValue.(string)
        if !ok {
            return errors.New("Default value should be of type varchar but is not")
        }

        if len(s) > int(newCol.Size) {
            return errors.New(fmt.Sprintf("Defaultvalue is too long: have %d, want %d", len(s), int(newCol.Size)))
        }

        btreeMoveSize = len(s) + 1
    } else if tp == types.VARCHAR {
        btreeMoveSize = 1 // write only \0
    }

    // check if default value matches with column type
    if isDefaultValue {
        err = validateDefaultValue(tp, int(newCol.Size), defaultValue)
        if err != nil {
            return err
        }
    }

    // Move btree entries
    // temp := int(colSize)
    fmt.Println("\n\n\n\n\n\nReached")
    entryList := &[]btree.Entry_t{}
    err = moveBtreeEntries(fh.Root, *fh.Root, entryList, int(colSize), btreeMoveSize)
    if err != nil {
        return err
    }

    if isDefaultValue {       
        // iterate over all entries, insert defaultValue for column 
        fmt.Println("Inserting default value \n\n\n\n")
        fmt.Println(defaultValue)
        err = insertDefaultValue(tb, fh, newCol, defaultValue)
        if err != nil {
            return err
        }
        tb.Columns = append(tb.Columns, newCol)
        return nil
    }
    
    // iterate over all entries, insert null for column 
    currentPos := tb.StartEntries
    values := [][][]byte{}
    for range tb.Entries.NumOfEntries {
        fmt.Println("Reading entry at", currentPos)
        buffer, err := entries.ReadEntryFromFile(tb, int(currentPos), fh)
        if err != nil {
            return err
        }
        values = append(values, buffer)
        currentPos += uint16(entries.GetEntryLength(buffer))
        fmt.Println("\n\n\nAllocating", newCol.Size, "Bytes at", currentPos)
        bytesWritten, err := appendNullValuesToFile(fh, &newCol, int64(currentPos))
        if err != nil {
            return err
        }
        currentPos += uint16(bytesWritten)
    }
    // append null values to end of file
    // This is necessary because method AllocateInFile() returns EOF for the last value
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()
    _, err = f.Seek(0, 2)
    if err != nil {
        return err
    }
    if newCol.Type == types.VARCHAR {
        _, err = f.Write([]byte("\000"))
        if err != nil {
            return err
        }
    } else {
        nullBytes := make([]byte, colSize)
        _, err = f.Write(nullBytes)
        if err != nil {
            return err
        }
    }

    tb.Columns = append(tb.Columns, newCol)
    return nil
}



// Returns number of bytes written and error
func appendNullValuesToFile (fh *entries.FileHandler, col *types.Column_t, currentPos int64) (int, error) {
    if col.Type == types.VARCHAR {
        err := AllocateInFile(fh, int64(currentPos), int64(1))
        if err != nil {
            return 0, err
        }
        return 1, nil
    }
    err := AllocateInFile(fh, int64(currentPos), int64(col.Size))
    if err != nil {
        return 0, err
    }
    return int(col.Size), nil
}


func existsColumnName (tb *types.Table_t, colName string) bool {
    for _, column := range tb.Columns {
        if column.Name == colName {
            return true
        }
    }
    return false
}


func insertColumnToFile (fh *entries.FileHandler, tb *types.Table_t, col *types.Column_t) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    if err = AllocateInFile(fh, int64(tb.StartEntries), int64(col.GetColSize())); err != nil {
        return err
    }
    fmt.Println(f)

    if err = WriteColumnAtOffset(fh, col, int64(tb.StartEntries)); err != nil {
        return err
    }

    return nil
}




func WriteColumnAtOffset (fh *entries.FileHandler, col *types.Column_t, offset int64) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
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



// allocates numBytes many Bytes in file from offset onwards
func AllocateInFile (fh *entries.FileHandler, offset int64, numBytes int64) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }

    tmp, err := os.OpenFile("./tmp.tb", os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer tmp.Close()
    // defer os.Remove(tmp.Name())
    
    if _, err := io.CopyN(tmp, f, offset); err != nil {
        return err
    }

    _, err = f.Seek(offset, 0)
    if err != nil {
        return err
    }
    
    _, err = tmp.Seek(offset + numBytes, 0)
    if err != nil {
        return err
    }

    if _, err := io.Copy(tmp, f); err != nil {
        return err
    }

    f.Close()

    err = os.Rename(tmp.Name(), fh.Path)
    if err != nil {
        return err
    }
    
    return nil
}




func moveBtreeEntries (root **btree.Node_t, current *btree.Node_t, entryList *[]btree.Entry_t, colSize int, colTypeSize int) error {
    fmt.Println("Moving btree entries")
    *entryList = createEntryListSortedByOffset(root, current, entryList)
    fmt.Println(entryList)
    err := updateBtreeValues(root, current, entryList, colSize, colTypeSize)
    if err != nil {
        return err
    }
    return nil
}


func updateBtreeValues(root **btree.Node_t, current *btree.Node_t, entryList*[]btree.Entry_t, colSize int, colTypeSize int) error {
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


func createEntryListSortedByOffset(root **btree.Node_t, current *btree.Node_t, entryList *[]btree.Entry_t) []btree.Entry_t {
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
func findIndex (arr []btree.Entry_t, key uint32) int {
    for i, entry := range arr {
        if entry.Key == key {
            return i
        }
    }
    return -1
}


func validateDefaultValue (colType types.Type_t, colSize int, defaultValue any) error {
    switch (colType){
    case types.INT32:
        _, ok := defaultValue.(int32)
        if !ok {
            return errors.New("Expected type to be int32. defaultvalue does not match")
        }
    case types.FLOAT32:
        _, ok := defaultValue.(float32)
        if !ok {
            return errors.New("Expected type to be float32. defaultvalue does not match")
        }
    case types.BOOL:
        _, ok := defaultValue.(bool)
        if !ok {
            return errors.New("Expected type to be bool. defaultvalue does not match")
        }
    case types.VARCHAR:
        s, ok := defaultValue.(string)
        if !ok {
            return errors.New("Expected type to be varchar. defaultvalue does not match")
        }
        if len(s) > colSize {
            return errors.New(fmt.Sprintf("Expected a varchar length of max %d but defaultvalue has a length of %d", colSize, len(s)))
        }
    }
    return nil
}



func insertDefaultValue(tb *types.Table_t, fh *entries.FileHandler, newCol types.Column_t, defaultValue any) error {
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
        buffer, err := entries.ReadEntryFromFile(tb, int(currentPos), fh)
        if err != nil {
            return err
        }
        values = append(values, buffer)
        currentPos += uint16(entries.GetEntryLength(buffer))
        fmt.Println("\n\n\nWriting", insertSize, "Bytes at", currentPos)
        fmt.Println("Writing", defaultValue)
        bytesWritten, err := writeInFile(fh, int64(currentPos), int64(insertSize), defaultValue, newCol.Type)
        if err != nil {
            return err
        }
        currentPos += uint16(bytesWritten)
    }

    // write default to EOF 
    err := writeToEOF(fh, defaultValue, newCol.Type)
    if err != nil {
        return err
    }

    return nil
}



func writeInFile(fh *entries.FileHandler, offset int64, numBytes int64, defaultValue any, dvType types.Type_t) (int, error){
    err := AllocateInFile(fh, offset, numBytes)
    if err != nil {
        return 0, err
    }

    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
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



func writeToEOF (fh *entries.FileHandler, defaultValue any, tp types.Type_t) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
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


