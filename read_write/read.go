package read_write

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/MalikL2005/Go_DB/types"
)

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
        // Read table
        err := binary.Read(f, binary.LittleEndian, &tb.NumOfColumns)
        if err != nil {
            fmt.Println("Err")
            fmt.Println(err)
            return err
        }
        fmt.Println("Read this from file")
        fmt.Println(tb.NumOfColumns)
        

        bytes, err := readStringFromFile (f, 10)
        if err != nil {
            return err
        }
        tb.Name = string(bytes)

        tb.Columns = make([]types.Column_t, tb.NumOfColumns)

        // read columns
        fmt.Println(tb.NumOfColumns)
        for i := range tb.NumOfColumns {
            offset, err := f.Seek(0, 1)
            if err != nil {
                fmt.Println("Error getting seek")
            }
            tb.Columns[i], err = ReadColumnFromFile(f, offset)
            if err != nil {
                fmt.Println(err)
            }
        }



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



func ReadColumnFromFile (f * os.File, offset int64) (types.Column_t, error) {
    _, err := f.Seek(offset, 0)
    if err != nil {
        fmt.Println("Error moving cursor in file")
        return types.Column_t{}, err
    }

    colBuffer := types.Column_t{}
    buf, err := readStringFromFile(f, 10)
    if err != nil {
        fmt.Println("Error reading colname")
        return types.Column_t{}, err
    }
    colBuffer.Name = string(buf)

    err = binary.Read(f, binary.LittleEndian, &colBuffer.Type)
    if err != nil {
        fmt.Println("Error reading coltype")
        fmt.Println(err)
        return types.Column_t{}, err
    }

    err = binary.Read(f, binary.LittleEndian, &colBuffer.Size)
    if err != nil {
        fmt.Println("Error reading coltype")
        fmt.Println(err)
        return types.Column_t{}, err
    }

    return colBuffer, nil
}
