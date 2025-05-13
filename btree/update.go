package btree

import(
    "fmt"
)

func UpdateBtreeOffset (root **Node_t, current *Node_t, offsetValue int64, fromOffsetOnwards uint32) {
    if current == nil {
        return
    }

    for i := range *current.Entries {
        if (*current.Entries)[i].Value >= fromOffsetOnwards {
            (*current.Entries)[i].Value += uint32(offsetValue)
        }
    }

    if current.Children == nil {
        return
    }
    for i, child := range *current.Children {
        fmt.Println("Traversing child no", i)
        if len(*current.Entries) == i || (*current.Entries)[i].Value >= fromOffsetOnwards{ // first condition is to always traverse the most right child
            UpdateBtreeOffset(root, &child, offsetValue, fromOffsetOnwards)
        }
    }

}



