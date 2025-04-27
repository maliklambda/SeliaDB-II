package btree

import (
	"errors"
	"fmt"
	"slices"
)



// Todo change function to take PK as parameter, not Entry_t
func Delete (root **Node_t, current *Node_t, entry Entry_t) error {
    if current == nil {
        return errors.New("Current may not be nil")
    }

    // current is leaf node and current has more than MIN_KEYS+1
    if (current.Children == nil || len(*current.Children) == 0) && len(*current.Entries) >= MIN_KEYS+1 {
        fmt.Println("Found right node")
        fmt.Println("Deleting", entry, "from", *current.Entries)
        // entry can simply be removed
        deleteIndex := slices.Index(*current.Entries, entry)
        if deleteIndex < 0 {
            return errors.New("Entry not found in current.Entries")
        }
        *current.Entries = slices.Delete(*current.Entries, deleteIndex, deleteIndex+1)
        current.NumOfEntries--
        return nil
    }

    // current is leaf node and current current has less than MIN_KEYS+1
    if (current.Children == nil || len(*current.Children) == 0) && len(*current.Entries) < MIN_KEYS+1 {
        return errors.New("Can't delete this")
    }

    if len(*current.Children) > 0 {
        childIndex := findChildIndex(*current, entry)
        return Delete(root, &(*current.Children)[childIndex], entry)
    }

    return nil
}
