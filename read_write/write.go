package read_write

import (
	"encoding/binary"
	"fmt"
	"os"
	"github.com/MalikL2005/Go_DB/types"
)



type fileHandler struct {
    Path string
    File *os.File
}



func OpenFile (fileName string) (fileHandler, error) {
    f, err := os.Create(fileName)
    if err != nil {
        return fileHandler{}, err
    }
    defer f.Close()
    return fileHandler{fileName, nil}, nil
}




func (fh fileHandler) WriteTableToFile (tb types.Table_t, offset int64) error {
    f, err := os.OpenFile(fh.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()
    _, err = f.Seek(offset, 0)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, tb.NumOfColumns)
    if err != nil {
        return err
    }

    _, err = f.Write([]byte(tb.Name + "\000"))
    if err != nil {
        return err
    }

    fh.File = f

    for _, col := range tb.Columns {
        offset, err := f.Seek(0,1)
        if err != nil {
            fmt.Println("Could not write this column to file.")
            continue
        }
        fmt.Printf("Offset: %d\n", offset)
        fh.WriteColumnToFile(col, offset)
    }

    return nil
}



func (fh fileHandler) WriteColumnToFile (col types.Column_t, offset int64) error{
    _, err := fh.File.Write([]byte(col.Name + "\000"))
    if err != nil {
        fmt.Println("Error writing col name to file")
        return err
    }

    err = binary.Write(fh.File, binary.LittleEndian, col.Type)
    if err != nil {
        fmt.Println("Error writing col type to file")
        fmt.Println(err)
        return err
    }

    err = binary.Write(fh.File, binary.LittleEndian, col.Size)
    if err != nil {
        fmt.Println("Error writing col size to file")
        return err
    }
    return nil

}



