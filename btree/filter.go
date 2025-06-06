package btree

import (
	"fmt"
	"github.com/MalikL2005/SeliaDB-II/types"
)




func TraverseWithFilter (root *Node_t, current *Node_t, resultEntries *[]Entry_t, fnCmp compareFunction, cmpOperator types.CompareOperator, valCmp uint32, fnDo doFunction) []Entry_t{
    if current == nil {
        return *resultEntries
    }

    for i := range *current.Entries {
        if fnCmp(&(*current.Entries)[i], valCmp, cmpOperator){
            fnDo(&(*current.Entries)[i])
            *resultEntries= append(*resultEntries, (*current.Entries)[i])
        }
    }

    if current.Children == nil {
        return *resultEntries
    }
    for _, child := range *current.Children {
        TraverseWithFilter(root, &child, resultEntries, fnCmp, cmpOperator, valCmp, fnDo)
    }

    return *resultEntries
}




// determins what to do with each entry, in addition to adding it to resultEntries (this is default behaviour)
type doFunction func(*Entry_t) error

func PrintEntry (entry *Entry_t) error {
    fmt.Println(*entry)
    return nil
}


//is used to enable passing a compare function to TraverseWithFilter()
type compareFunction func(*Entry_t, uint32, types.CompareOperator) bool 


func CompareBtreeValues (entry *Entry_t, cmpVal uint32, cmpOperator types.CompareOperator) bool {
    switch(cmpOperator){
        case types.GREATER: return entry.Value > cmpVal
        case types.SMALLER:return entry.Value < cmpVal
        case types.EQUAL:return entry.Value == cmpVal
        case types.SMALLER_EQUAL: return entry.Value <= cmpVal
        case types.GREATER_EQUAL: return entry.Value >= cmpVal
    }
    return false
}


func CompareBtreeKeys (entry *Entry_t, cmpVal any, cmpOperator types.CompareOperator, tp types.Type_t) bool {
    switch(cmpOperator){
        case types.GREATER: 
            res, _:= types.CompareAnyValues(entry.Key, cmpVal, tp)
            return res > 0
        case types.SMALLER:
            res, _:= types.CompareAnyValues(entry.Key, cmpVal, tp)
            return res < 0
        case types.EQUAL:
            res, _:= types.CompareAnyValues(entry.Key, cmpVal, tp)
            return res == 0
        case types.SMALLER_EQUAL: 
            res, _:= types.CompareAnyValues(entry.Key, cmpVal, tp)
            return res <= 0
        case types.GREATER_EQUAL: 
            res, _:= types.CompareAnyValues(entry.Key, cmpVal, tp)
            return res >= 0
    }
    return false
}



