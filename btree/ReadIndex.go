package btree

import (
	"encoding/binary"
	"fmt"
	"os"
)



func ReadIndexFromFile(tbName, colName string) (*Node_t, error) {
    fmt.Println("Looking for this file to read index from:", fmt.Sprintf("%s_%s.index", tbName, colName))
    return nil, nil
}



func ReadIndex(root **Node_t, current *Node_t) (Node_t, error){
    f, err := os.Open(indexFileName)
    if err != nil {
        return Node_t{}, err
    }
    var newRoot Node_t
    err = binary.Read(f, binary.LittleEndian, &newRoot.NumOfEntries)
    if err != nil {
        return Node_t{}, err
    }
    fmt.Println(newRoot.NumOfEntries)

    newEntries := make([]Entry_t, newRoot.NumOfEntries)
    for i := range newRoot.NumOfEntries {
        newEntries[i] = readEntry(f)
    }
    fmt.Println(newEntries)
    newRoot.Entries = &newEntries

    return newRoot, nil
}


func readEntry(f *os.File) Entry_t {
    if f == nil {
        return Entry_t{}
    }
    entr := Entry_t{}
    binary.Read(f, binary.LittleEndian, &entr)
    fmt.Println(entr)
    return entr
}
