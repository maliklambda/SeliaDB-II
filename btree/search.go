package btree


func SearchKey (root **Node_t, current *Node_t, searchedKey uint32) *Entry_t {
    if current == nil {
        return nil
    }
    found, childIndex := isKeyInNode(current, searchedKey)
    if found {
        return &(*current.Entries)[childIndex]
    }
    if current.Children == nil || len(*current.Children) == 0 {
        return nil
    }
    return SearchKey(root, &(*current.Children)[childIndex], searchedKey)

}


func isKeyInNode(current *Node_t, searchedKey uint32) (bool, int) {
    i := 0
    for _, entry := range *current.Entries {
        if entry.Key == searchedKey {
            return true, i
        }
        if entry.Key > searchedKey {
            return false, i
        }
        i++
    }
    return false, 0
}
