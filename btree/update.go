package btree

import (
	"fmt"
	"maps"
	"slices"
	"sort"
)

func UpdateBtreeOffset (root *Node_t, current *Node_t, offsetValue int64, fromOffsetOnwards uint32) {
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



func UpdateBtreeOffsetMap (root *Node_t, offsets *map[int]int32) {
    sl := slices.Collect(maps.Keys(*offsets))
    sort.Ints(sl)
    iterateOverBtreeEntriesUpdateMap(root, root, offsets, &sl)
}



func iterateOverBtreeEntriesUpdateMap (root *Node_t, current *Node_t, offsets *map[int]int32, offsetsSlice *[]int) {
    if current == nil {
        return
    }

    for i := range *current.Entries {
        fmt.Println((*current.Entries)[i])
        newIndex := findIndexInOffsets((*current.Entries)[i].Value, offsetsSlice, offsets)
        fmt.Println(newIndex)
        if len(*offsetsSlice) < newIndex && newIndex >= 0 {
            // check for if index > len(slice)
            fmt.Println((*offsetsSlice)[newIndex])
            fmt.Println((*offsets)[(*offsetsSlice)[newIndex]])
            (*current.Entries)[i].Value += uint32((*offsets)[(*offsetsSlice)[newIndex]])
        } else if newIndex >= 0 { // so len(*offsetsSlice) >= newIndex
            fmt.Println()
            fmt.Println((*offsets)[(*offsetsSlice)[len(*offsetsSlice)-1]])
            (*current.Entries)[i].Value += uint32(((*offsets)[(*offsetsSlice)[len(*offsetsSlice)-1]]))
        }
    }

    if current.Children == nil {
        return
    }
    for i, child := range *current.Children {
        fmt.Println("updating offset for child no", i)
        iterateOverBtreeEntriesUpdateMap(root, &child, offsets, offsetsSlice)
    }
}


// expects len(offsets) to be grater or equal to one
func findIndexInOffsets (value uint32, offsetsSlice *[]int, offsetMap *map[int]int32) int {
    fmt.Println("searching for", value, "in", *offsetsSlice)
    _, ok := (*offsetMap)[int(value)]
    if ok {
        return slices.Index(*offsetsSlice, int(value))
    }
    index := sort.SearchInts(*offsetsSlice, int(value))
    if index == 0 && value < uint32((*offsetsSlice)[0]){
        return -1
    }
    return index
}



