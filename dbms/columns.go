package dbms

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"


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
    if err = insertColumnToFile(tb, newCol); err != nil {
        return err
    }
    if err = entries.UpdateNumOfColumns(tb, tb.NumOfColumns+1); err != nil {
        return err
    }



    fmt.Println("New Start entries", tb.StartEntries)

    var defaulLength int
    var defaultValueBt []byte
    switch tp {
    case types.VARCHAR:
        default_string, ok := defaultValue.(string)
        if !ok {
            return errors.New("Expected defaultValue to be of type VARCHAR.")
        }
        defaulLength = len(default_string + "\000")
        defaultValueBt = []byte(default_string+"\000")

    case types.INT32, types.FLOAT32:
        default_int32, ok := defaultValue.(int32)
        if !ok {
            return errors.New("Expected defaultValue to be of type INT32.")
        }
        defaulLength = binary.Size(int32(1))
        defaultValueBt = make([]byte, defaulLength)
        binary.LittleEndian.PutUint32(defaultValueBt, uint32(default_int32))

    case types.BOOL:
        defaulLength = binary.Size(true)

    default: return errors.New("invalid type")
    }

    fmt.Println("Default infos:")
    fmt.Println("defaultvalue:", defaultValueBt)
    fmt.Println("default length:", defaulLength)
    fmt.Println("type:", tb)

    // btreeOffsetUpdate := make(map[int]int32)
    // append either null values or default value
    currentPos := tb.StartEntries
    var savePos int64 // needed to store info at eof
    for {
        val, pNextEntry, err := entries.ReadEntryFromFile(tb, int(currentPos))
        if err != nil {
            fmt.Println(currentPos)
            fmt.Println(savePos)
            
            f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
            if err != nil {
                return err
            }
            eof, _ := f.Seek(0, 2)
            fmt.Println("eof:", eof)
            if _, err = f.Seek(savePos, 0); err != nil {
                return err
            }
            if err = binary.Write(f, binary.LittleEndian, (currentPos+uint16(types.GetEntryBuffer()))-uint16(savePos)-uint16(types.PNextEntrySize)); err != nil {
                return err
            }
            f.Close()
            fmt.Println("exiting\n\n ")
            break
        }
        entryLength := entries.GetEntryLength(val)
        if int(currentPos) + entryLength + defaulLength >= int(pNextEntry) {
            fmt.Println("Buffer is full")
            err = types.AllocateInFile(tb.MetaData.FilePath, int64(currentPos)+int64(entryLength)+int64(binary.Size(types.PNextEntrySize)), int64(types.GetEntryBuffer()))
            if err != nil {
                fmt.Println("\n\n\n\nerror:", err)
                break
            }
            pNextEntry += int64(types.GetEntryBuffer())
            // update btree values
        } else {
            fmt.Println("buffer is not full")
            fmt.Println(currentPos)
            fmt.Println(entryLength)
            fmt.Println(pNextEntry)
        }

        // write defaultValue
        f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            return err
        }
        if _, err = f.Seek(int64(currentPos)+int64(entryLength), 0); err != nil {
            return err
        }

        fmt.Println("writing defaultValue of length", defaulLength, "@", int(currentPos)+entryLength)
        if _, err = f.Write(defaultValueBt); err != nil {
            return err
        }

        pos, _ := f.Seek(0, 1)
        fmt.Println("writing pNextEntry", pNextEntry, "@", pos)
        if err = binary.Write(f, binary.LittleEndian, pNextEntry-pos-types.PNextEntrySize); err != nil {
            return err
        }
        f.Close()
        // e, ne, _ := entries.ReadEntryFromFile(tb, int(currentPos))
        // fmt.Println("\n\n\n\nREad this entry", e)
        // fmt.Println("Next one @", ne)

        savePos = int64(currentPos) + int64(entries.GetEntryLength(val)) + int64(defaulLength)
        currentPos = uint16(pNextEntry)
    }
    
    // write pointer to next (not yet existing entr)
    fmt.Println("\n\n\nended on currentPos", currentPos)
        f, err := os.OpenFile(tb.MetaData.FilePath, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            return err
        }
        if _, err := f.Seek(int64(currentPos-uint16(types.PNextEntrySize)), 0); err != nil {
            return err
        }
        // if err = binary.Write(f, binary.LittleEndian, uint16(types.GetEntryBuffer())); err != nil {
        if err = binary.Write(f, binary.LittleEndian, uint16(600)); err != nil {
            return err
        }
        f.Close()

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
    f.Close()

    fmt.Println("hereree", tb.EndOfTableData+ uint16(binary.Size(col)))
    if err = entries.UpdateEndOfTableData(tb, tb.EndOfTableData+ uint16(col.GetColSize())); err != nil {
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

    if tb.NumOfColumns == 1 {
        fmt.Println("Just delete everything brooooo")
        return errors.New("Num of cols is just one")
    }

    // read all columns up to index
    f, err := os.Open(tb.MetaData.FilePath)
    if err != nil {
        fmt.Println("errored here")
        return err
    }
    defer f.Close()

    // Read table
    curPos, err := f.Seek(int64(binary.Size(tb.NumOfColumns)+len(tb.Name+"\000")+binary.Size(tb.EndOfTableData)+binary.Size(tb.StartEntries)), 0)
    if err != nil {
        return err
    }
    fmt.Println("starting columns @", curPos)

    // read columns
    var colBuffer types.Column_t
    nextOffset := curPos
    for range index +1 {
        colBuffer, err = entries.ReadColumnFromFile(f, nextOffset)
        if err != nil {
            return err
        }
        nextOffset += int64(colBuffer.GetColSize())
    }
    fmt.Println(colBuffer.GetColSize())
    fmt.Println(colBuffer)
    colLength := int64(colBuffer.GetColSize())

    fmt.Println("startOffset", nextOffset)
    fmt.Println("colLength", colLength)
    f.Close()
    // delete colName from file
    if err = entries.DeleteBytesFromTo(tb.MetaData.FilePath, int64(nextOffset-colLength), int64(nextOffset)); err != nil {
        return err
    }

    // delete entries for deleted column from file
    // todo: delete multiple columns
    var val [][]byte
    var pNextEntry int64
    curOffset := int64(tb.StartEntries) - colLength
    for {
        val, pNextEntry, err = entries.ReadEntryFromFile(tb, int(curOffset))
        if err != nil {
            break
        }
        if err = entries.DeleteBytesFromTo(tb.MetaData.FilePath, int64(curOffset)+int64(entries.GetEntryLength(val[0:index])), int64(curOffset)+int64(entries.GetEntryLength(val[0:index+1]))); err != nil{
            return err
        }
        deletedBytes := int64(entries.GetEntryLength(val[0:index+1])) - int64(entries.GetEntryLength(val[0:index]))
        curOffset = pNextEntry - deletedBytes
        fmt.Println("here:", curOffset)
    }


    // update EndOfTableData
    if err = entries.UpdateEndOfTableData(tb, tb.EndOfTableData-uint16(colLength)); err != nil {
        return err
    }

    //update start entries
    if err = entries.UpdateStartEntries(tb, tb.StartEntries-uint16(colLength)); err != nil {
        return err
    }

    // update NumOfColumns
    err = entries.UpdateNumOfColumns(tb, tb.NumOfColumns-1)
    if err != nil {
        return err
    }

    tb.Columns = slices.Concat(tb.Columns[:index], tb.Columns[index+1:])

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



