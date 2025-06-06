package btree

import (
	"errors"
	"fmt"

	"github.com/MalikL2005/SeliaDB-II/types"
)


func SearchKey (root *Node_t, current *Node_t, searchedKey any, tp types.Type_t) (*Entry_t, error) {
    if current == nil {
        return nil, errors.New("Nothing found")
    }
    found, childIndex := isKeyInNode(current, searchedKey, tp)
    if found {
        return &(*current.Entries)[childIndex], nil
    }
    if current.Children == nil || len(*current.Children) == 0 {
        return nil, errors.New("Nothing found")
    }
    return SearchKey(root, &(*current.Children)[childIndex], searchedKey, tp)

}


func isKeyInNode(current *Node_t, searchedKey any, tp types.Type_t) (bool, int) {
    i := 0
    for _, entry := range *current.Entries {
        if entry.Key == searchedKey {
            return true, i
        }
        fmt.Println("Comparing ", entry.Key, " and ", searchedKey)
        if res, _ := types.CompareAnyValues(entry.Key, searchedKey, tp); res > 0 {
            return false, i
        } else if res == 0 {
            return true, i
        }
        i++
    }
    return false, i
}
