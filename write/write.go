package write

import (
	"encoding/binary"
	"errors"
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
        fh.WriteColumnToFile(col, offset)
    }

    return nil
}



func (fh fileHandler) WriteColumnToFile (col types.Column_t, offset int64){
    fmt.Println("Writing col to file")
    fmt.Println(col)
    fmt.Printf("Writing at %d\n", offset)

}



func (fh fileHandler) ReadFromFile (data any, offset int64) error {
    f, err := os.Open(fh.Path)
    if err != nil {
        return err
    }
    defer f.Close()

    _, err = f.Seek(offset, 0)
    if err != nil {
        return err
    }

    tb, ok := data.(*types.Table_t)
    if ok {
        err := binary.Read(f, binary.LittleEndian, &tb.NumOfColumns)
        if err != nil {
            fmt.Println("Err")
            fmt.Println(err)
            return nil
        }
        fmt.Println("Read this from file")
        fmt.Println(tb.NumOfColumns)
        

        bytes, err := readStringFromFile (f, 10)
        tb.Name = string(bytes)

        return nil
    }

    _, ok = data.(types.Column_t)
    if ok {
        
        return nil
    }

    _, ok = data.(types.Database_t)
    if ok {
        
        return nil
    }

    return errors.New("Cannot read this type/Invalid data")
}




func readStringFromFile (f *os.File, MAX_LEN int) ([]byte, error) {
    var bytes []byte
    buf := make([]byte, 1)
    for range MAX_LEN {
        _, err := f.Read(buf)
        if err != nil && err.Error() == "EOF" {
            break
        } else if err != nil {
            return nil, err
        }

        if buf[0] == 0 {
            break
        }
        bytes = append(bytes, buf[0])
    }
    return bytes, nil
}



