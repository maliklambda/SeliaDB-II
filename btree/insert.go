package btree

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/types"
)

// root must not be nil
func Insert (root **Node_t, current *Node_t, entry Entry_t, tp types.Type_t) error {
    fmt.Println("Inserting!!!!", entry.Key)
    if (*root).Entries == nil {
        fmt.Println("Root entries is null")
        entries := make([]Entry_t, 1, MAX_KEYS)
        entries[0] = entry
        children := make([]Node_t, 0, MAX_CHILDREN)
        *root = &Node_t{Entries: &entries, Children: &children, NumOfEntries: 1}
        return nil
    }
    if current.Children == nil {
        newChildren := make([]Node_t, 0)
        current.Children = &newChildren
    }
    if current.Entries == nil {
        newEntries := make([]Entry_t, 0)
        current.Entries = &newEntries
    }

    // current is not a leaf node
    if len(*current.Children) != 0 {
        childIndex := findChildIndex(*current, entry, tp)
        if childIndex < 0 {
            return nil
        }
        err := Insert(root, &(*current.Children)[childIndex], entry, tp)
        if err != nil {
            return err
        }
        return nil
    }
    
    // current is leaf node and not full
    if len(*current.Children) == 0 && len(*current.Entries) < MAX_KEYS {
        fmt.Println("No children + root is not full")
        err := insertEntry(current, entry, tp)
        if err != nil {
            root = nil
            return err
        }

        current.NumOfEntries++
        return nil
    }

    // root is full
    if current == *root && len(*(*root).Entries) >= MAX_KEYS && len(*current.Children) == 0 {
        fmt.Println("root is full")
        tempArr, middleIndex, err := createTempArr(**root, entry, tp)
        if err != nil {
            fmt.Println("Error creating tempArray:", err)
            return err
        }
        fmt.Println("Splitting on", tempArr[middleIndex])
        fmt.Println("New Left:", tempArr[:middleIndex])
        fmt.Println("New right:", tempArr[middleIndex+1:])
        splitRoot(root, entry, tp)
        return nil
    }

    // current is leaf node and is full
    if len(*current.Children) == 0 && len(*current.Entries) >= MAX_KEYS {
        fmt.Println("Current is leaf and full")

        tempArr, _, err := createTempArr(*current, entry, tp)
        if err != nil {
            fmt.Println("error:", err)
            return err
        }

        fmt.Println(tempArr)
        parent, parentIndex := findParent(*root, current, tp)
        if parent == nil {
            return errors.New("No parent found")
        }
        fmt.Println("Parent", (*parent.Entries)[0], "at", parentIndex)
        borrowed, err := borrowFromSibling (current, parent, tempArr, parentIndex, tp)
        if err != nil {
            fmt.Println("errorrre:", err)
            return err
        }

        if borrowed {
            fmt.Println("Borrowed successfully")
            return nil
        }

        
        if len(*parent.Children) >= MAX_CHILDREN {
            fmt.Println("Want to split node but MAX Children is already reached")
            fmt.Println(tempArr)
            fmt.Println(parent.Entries)
            pushChildUp(root, current, entry, tp)
            return nil
        }

        // Split current node into two
        fmt.Println(tempArr)
        middleIndex := len(*(*root).Entries) / 2 +1 // +1 because the entry has a larger key in most cases
        newRightEntries := tempArr[middleIndex+1:]
        newRight := Node_t{Entries: &newRightEntries}
        // move parent->Children on place where newRight is to be inserted
        *parent.Children = slices.Insert(*parent.Children, parentIndex+1, newRight)
        *parent.Entries = slices.Insert(*parent.Entries, parentIndex, tempArr[middleIndex])
        *current.Entries = tempArr[:middleIndex]
        fmt.Println(*current.Entries)

        return nil

    }

    // root has children
    if current == *root && current.Children != nil {
        fmt.Println("Entry is to be inserted in child of root")
        childIndex := findChildIndex(**root, entry, tp)
        fmt.Println(childIndex)
        err := Insert(root, &(*current.Children)[childIndex], entry, tp)
        if err != nil {
            return err
        }
        return nil
    }

    return nil
}



func borrowFromSibling (current *Node_t, parent *Node_t, tempArr []Entry_t, parentIndex int, tp types.Type_t) (bool, error) {
    canBorrowFromLeftSibling := checkLeftSibling(parent, parentIndex)
    if canBorrowFromLeftSibling {
        fmt.Println("Borrowing from left sibling")
        leftSibling := (*parent.Children)[parentIndex-1]
        err := insertToNode(&leftSibling, (*parent.Entries)[parentIndex-1], tp)
        if err != nil {
            fmt.Println("error inserting to left sibling")
            return false, err
        }
        (*parent.Entries)[parentIndex-1] = tempArr[0]
        // move temparr left
        *current.Entries = tempArr[1:]
        fmt.Println("End:", current.Entries)
        return true, nil
    }
    fmt.Println("Can't borrow from left sibling")

    canBorrowFromRightSibling := checkRightSibling(parent, parentIndex)
    if canBorrowFromRightSibling {
        fmt.Println("Borrowing from right sibling")
        return false, errors.New("Not implemented yet")
    }
    return false, nil
}



func splitNode (root **Node_t, parent *Node_t, current *Node_t, entry Entry_t, parentIndex int, tp types.Type_t) error {
    if current == *root {
        fmt.Println("Current is root")
        return nil
    }
    tempArr, middleIndex, err := createTempArr(*current, entry, tp)
    if err != nil {
        fmt.Println("Error creating temparr")
        return err
    }

    newLeftEntries := tempArr[:middleIndex]
    newRightEntries := tempArr[middleIndex+1:]

    newLeft := Node_t{Entries: &newLeftEntries}
    newRight := Node_t{Entries: &newRightEntries}

    if len(*current.Children) > middleIndex +1 {
        fmt.Println("Middlelleeeeee index",middleIndex)
        newLeftChildren := (*current.Children)[:middleIndex+1]
        newRightChildren := (*current.Children)[middleIndex+1:]
        newLeft.Children = &newLeftChildren
        newRight.Children = &newRightChildren
    }

    (*parent.Children)[parentIndex] = newLeft
    *parent.Children = slices.Insert(*parent.Children, parentIndex+1, newRight)

    return nil
}



func pushChildUp (root **Node_t, current *Node_t, entry Entry_t, tp types.Type_t) error {
    if current == nil {
        fmt.Println("OHOH")
        return errors.New("current node is nil")
    }

    // check if current->entries is full
    if len(*current.Entries) < MAX_KEYS {
        fmt.Println("Inserting", entry, "to", *current.Entries)
        err := insertToNode (current, entry, tp)
        if err != nil {
            return errors.New(fmt.Sprint("Error inserting", entry, "into", *current.Entries))
        }
        return nil
    }
    parent, parentIndex := findParent(*root, current, tp)
    if parent == nil {
        splitRoot(root, entry, tp)
        return nil
    }

    tempArr, middleIndex, err := createTempArr(*current, entry, tp)
    if err != nil {
        return errors.New("Error creating temparr")
    }

    err = splitNode(root, parent, current, entry, parentIndex, tp)
    if err != nil {
        return errors.New(fmt.Sprint("Error in splitnode", err))
    }

    pushChildUp(root, parent, tempArr[middleIndex], tp)

    return nil
}


func splitRoot (root **Node_t, entry Entry_t, tp types.Type_t){
    fmt.Println("Splitting root, with", entry)
    tempArr, middleIndex, err := createTempArr(**root, entry, tp)
    if err != nil {
        fmt.Println("Error creating temp arr: fatal:", err)
        return
    }
    // new left
    newLeftEntries := tempArr[:middleIndex]
    newLeft := Node_t{Entries: &newLeftEntries, NumOfEntries: uint16(len(newLeftEntries))}

    // new right
    newRightEntries := tempArr[middleIndex+1:]
    newRight := Node_t{Entries: &newRightEntries, NumOfEntries: uint16(len(newRightEntries))}

    if len(*(*root).Children) >= middleIndex+1 {
        newLeftChildren := (*(*root).Children)[:middleIndex+1]
        newLeft.Children = &newLeftChildren

        newRightChildren := (*(*root).Children)[middleIndex+1:]
        newRight.Children = &newRightChildren
    }

    // new root
    newRootEntries := make([]Entry_t, 0, MAX_KEYS)
    newRootEntries = append(newRootEntries, tempArr[middleIndex])
    newRootChildren := make([]Node_t, 0, MAX_CHILDREN)
    newRootChildren = append(newRootChildren, newLeft, newRight)
    newRoot := Node_t{Entries: &newRootEntries, Children: &newRootChildren, NumOfEntries: uint16(len(newRootEntries))}
    *root = &newRoot
}



func createTempArr (n Node_t, entry Entry_t, tp types.Type_t) ([]Entry_t, int, error) {
    if entry == (Entry_t{}) {
        return []Entry_t{}, 0, errors.New("Entry must not be empty Entry_t")
    }
    tempArr := insertToTempArr(n, entry, tp)
    if len(tempArr) == 0{
        return []Entry_t{}, 0, errors.New("Failed to create temporary array")
    }
    middleIndex := len(*n.Entries) / 2 +1 // +1 because the entry has a larger key in most cases
    return tempArr, middleIndex, nil
}


func insertToTempArr (n Node_t, entry Entry_t, tp types.Type_t) []Entry_t {
    tempArr := make([]Entry_t, len(*n.Entries)+1)
    i := 0
    switch tp {
    case types.INT32:
        for _, nodeEntry := range *n.Entries {
            fmt.Println(nodeEntry.Key, entry.Key)
            if nodeEntry.Key.(int32) >= entry.Key.(int32) {
                if nodeEntry.Key == entry.Key { // this should not be!
                    return []Entry_t{}
                }
                tempArr[i] = entry
                i++
                continue
            }
            tempArr[i] = nodeEntry
            i++
        }
    case types.FLOAT32:
        for _, nodeEntry := range *n.Entries {
            if nodeEntry.Key.(float32) >= entry.Key.(float32) {
                if nodeEntry.Key == entry.Key { // this should not be!
                    return []Entry_t{}
                }
                tempArr[i] = entry
                i++
                continue
            }
            tempArr[i] = nodeEntry
            i++
        }
    case types.VARCHAR:
        for _, nodeEntry := range *n.Entries {
            fmt.Println("Comparing: ", nodeEntry.Key.(string), " and ", entry.Key.(string))
            if cmp := strings.Compare(nodeEntry.Key.(string), entry.Key.(string)); cmp >=0 {
                if cmp == 0 { // this should not be!
                    return []Entry_t{}
                }
                tempArr[i] = entry
                i++
                tempArr = append(tempArr, (*n.Entries)[i-1:]...)
                tempArr = slices.DeleteFunc(tempArr, func(entry Entry_t) bool {return entry.Key == nil})
                break
            }
            fmt.Println(entry.Key.(string), " is greater")
            fmt.Println(nodeEntry)
            tempArr[i] = nodeEntry
            i++
        }
    }
    fmt.Println(tempArr)
        
    // check if entry has not been inserted
    if tempArr[len(*n.Entries)] == (Entry_t{}) {
        tempArr[len(*n.Entries)] = entry
    }
    fmt.Println("Temp Arr:", tempArr)
    return tempArr
}



func findChildIndex (current Node_t, entry Entry_t, tp types.Type_t) int {
    fmt.Println("Finding child index")
    i := 0
    for _, nodeEntry := range *current.Entries {
        if res, err := types.CompareAnyValues(nodeEntry.Key, entry.Key, tp); res >= 0 {
            if nodeEntry.Key == entry.Key || err != nil { // this should not be!
                return -1
            }
            return i
        }
        i++
    }
    return i
}



func findParent (current *Node_t, goal *Node_t, tp types.Type_t) (*Node_t, int) {
    if current.Children == nil || len(*current.Children) == 0 {
        return nil, 0
    }
    if goal == nil {
        return nil, 0
    }
    for i, child := range *current.Children {
        if child == *goal {
            return current, i
        }
    }
    childIndex := findChildIndex(*current, (*goal.Entries)[0], tp)
    if childIndex >= len(*current.Children){
        fmt.Println("Error finding child index")
        return nil, 0
    }
    return findParent(&(*current.Children)[childIndex], goal, tp)
}



func checkLeftSibling (parent *Node_t, parentIndex int) bool {
    if parentIndex < 1 {
        return false
    }
    leftSibling := (*parent.Children)[parentIndex-1]
    if len(*leftSibling.Entries) < MAX_KEYS {
        return true
    }
    return false
}



func checkRightSibling (parent *Node_t, parentIndex int) bool {
    if parentIndex < 1 {
        return false
    }
    rightSibling := (*parent.Children)[parentIndex]
    if len(*rightSibling.Entries) < MAX_KEYS {
        return true
    }
    return false
}


func insertToNode (current *Node_t, entry Entry_t, tp types.Type_t) error {
    fmt.Println("Inserting to node")
    fmt.Println(entry)
    if len(*current.Entries) >= MAX_KEYS {
        return errors.New("Can't insert to full node you dummy")
    }
    i := 0
    for _, nodeEntry := range *current.Entries {
        if res, err := types.CompareAnyValues(nodeEntry.Key, entry.Key, tp); res > 0 {
            break
        } else if res < 0 {
            i++
        } else if err != nil {
            return err
        } else { // so pk already exists
            return errors.New("Duplicate PK")
        }
    }
    *current.Entries = slices.Insert(*current.Entries, i, entry)
    fmt.Println(current.Entries)
    
    return nil
}


func insertEntry (current *Node_t, entry Entry_t, tp types.Type_t) error {
    fmt.Println("Inserting Entry")
    i := 0
    for ; i<len(*current.Entries); i++ {
        if res, _ := types.CompareAnyValues((*current.Entries)[i].Key, entry.Key, tp); len(*current.Entries)<=i || res >= 0 {
            if res == 0 {
                return errors.New(fmt.Sprint("Cannot index because value ", entry.Key, " already exists"))
            }
            break
        }
    }
    
    *current.Entries = append(*current.Entries, Entry_t{})
    copy((*current.Entries)[i+1:], (*current.Entries)[i:])
    (*current.Entries)[i] = entry
    fmt.Println((*current.Entries))
    fmt.Println(i)
    fmt.Println(*current.Entries)
    fmt.Println(entry)
    fmt.Println("Inserting at index", i)
    return nil
}



