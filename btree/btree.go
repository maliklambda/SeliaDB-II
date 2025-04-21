package btree

import "fmt"


type Node_t struct {
    NumOfEntries uint16
    Entries *[]Entry_t
    Children *[]Node_t
}


type Entry_t struct {
    Key uint32
    Value uint32 // offset to where the entry is stored in the file
}


const (
    C = 2
    MIN_CHILDREN = C
    MAX_CHILDREN = 2*C // MAX_CHILDREN = 2 * C
    MIN_KEYS = C-1 // MIN_KEYS = C - 1
    MAX_KEYS = C*2 -1 // MAX_KEYS = 2 * MIN_CHILDREN - 1
)


func Traverse(root *Node_t, current *Node_t){
    if current == nil {
        return
    }

    fmt.Println("New Node", current.NumOfEntries)
    for i, entry := range *current.Entries {
        fmt.Println(i, ":", entry)
    }

    if current.Children == nil {
        return
    }
    for i, child := range *current.Children {
        fmt.Println("Traversing child no", i)
        Traverse(root, &child)
    }

}


