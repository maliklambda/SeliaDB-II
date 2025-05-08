package entries

import (
	"fmt"
	"io"
	"os"

	"github.com/MalikL2005/SeliaDB-II/btree"
	"github.com/MalikL2005/SeliaDB-II/types"
    "errors"
)

func DeleteAllEntries (tb *types.Table_t, fh *FileHandler) error {
    if tb.Entries != nil && tb.Entries.NumOfEntries == 0 {
        return nil
    }
    f, err := os.OpenFile(fh.Path, os.O_RDWR, 0644)
    if err != nil {
        return err
    }
    defer f.Close()
    end, err := f.Seek(0, 2)
    if err != nil {
        return err
    }
    err = DeleteBytesFromTo(fh, int64(tb.StartEntries), end)
    if err != nil {
        return err
    }
    UpdateOffsetLastEntry(fh, 0)
    return nil
}



func DeleteBytesFromTo (fh *FileHandler, from, to int64) error {
    fmt.Println("Deleting from", from, "to", to)
    f, err := os.Open(fh.Path)
    if err != nil {
        return err
    }
    defer f.Close()

    tmp, err := os.CreateTemp("", "tmp-" + fh.Path)
    if err != nil {
        return err
    }
    defer tmp.Close()

    _, err = io.CopyN(tmp, f, from)
    if err != nil {
        return err
    }

    _, err = f.Seek(to, 0)
    if err != nil {
        return err
    }

    _, err = io.Copy(tmp, f)
    if err != nil {
        return err
    }

    fmt.Println(tmp)
    tmp.Close()
    f.Close()

    err = os.Rename(tmp.Name(), fh.Path)
    if err != nil {
        return err
    }

    return nil
}


func DeleteEntryByPK (tb *types.Table_t, fh *FileHandler, pk uint32) error {
    entry := btree.SearchKey(fh.Root, *fh.Root, pk)
    if entry == nil {
        return errors.New("PK was not found")
    }

    values, err := ReadEntryFromFile(tb, int(entry.Value), fh)
    if err != nil {
        return err
    }
    
    length := GetEntryLength(values)
    if length == 0 {
        return errors.New("length of entry returned 0")
    }

    err = DeleteBytesFromTo(fh, int64(entry.Value), int64(int(entry.Value)+length))
    if err != nil {
        return err
    }

    // Remove pk from btree
    // btree.Delete(fh.Root, *fh.Root, pk)

    // Update Btree.values where value > entry.Value


    return nil
}




