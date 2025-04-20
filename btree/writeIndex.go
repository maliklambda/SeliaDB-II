package btree

import (
	"encoding/binary"
	"fmt"
	"os"
)

const indexFileName = "test.index"

func WriteBtree(root **Node_t, current *Node_t) error {
    f, err := os.OpenFile(indexFileName, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    err = writeNode(current, f)
    if err != nil {
        fmt.Println("errorrrrrrr", err)
        return err
    }

    return nil
}

func writeNode(current *Node_t, f *os.File) error {
    err := binary.Write(f, binary.LittleEndian, current.NumOfEntries)
    if err != nil {
        return err
    }

    err = binary.Write(f, binary.LittleEndian, *(current.Entries))
    if err != nil {
        return err
    }

    if current.Children == nil || len(*current.Children)==0 {
        fmt.Println("Child is null")
        _, err = f.Write([]byte("Ends here"))
        if err != nil {
            return err
        }
        return nil
    }
    children := make([]uint64, current.NumOfEntries+1)
    err = binary.Write(f, binary.LittleEndian, children)
    if err != nil {
        return err
    }

    return nil
}

func TraverseWrite(root *Node_t, current *Node_t, f *os.File){
    if current == nil {
        return
    }

    fmt.Println("New Node", current.NumOfEntries)
    for i, entry := range *current.Entries {
        fmt.Println(i, ":", entry)
        err := binary.Write(f, binary.LittleEndian, (*current.Entries)[i])
        if err != nil {
            return
        }
    }

    if current.Children == nil {
        return
    }

    for i, child := range *current.Children {
        fmt.Println("Traversing child no", i)
        Traverse(root, &child)
    }

}


