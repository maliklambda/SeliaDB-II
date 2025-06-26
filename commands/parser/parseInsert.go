package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/MalikL2005/SeliaDB-II/types"
)

const (
    INTO = SPACE+"INTO"+SPACE
    VALUES = SPACE+"VALUES"+SPACE
)

func ParseInsert (query string, db *types.Database_t) (string, *map[string] string, error) { // values = map [colName] value
    if query[:len(INTO)] != INTO {
        return "", nil, errors.New("Expected INTO")
    }
    query = query[len(INTO):]

    nextSpace := strings.Index(query, SPACE)
    if nextSpace <= 0 {
        return "", nil, errors.New("Invalid tableName")
    }
    tbName := query[:nextSpace]
    fmt.Println(tbName)
    query = query[nextSpace:]

    if query[:len(VALUES)] != VALUES {
        return "", nil, errors.New("Expected VALUES")
    }
    query = query[len(VALUES):]

    if query[0] != '(' || query[len(query)-1] != ')' {
        return "", nil, errors.New("Expected values to be wrapped in parenthesis")
    }
    
    query = query[1:len(query)-1]
    parts := strings.Split(query, ","+SPACE)
    fmt.Println(parts)

    var err error
    colVals := make(map[string]string)
    for _, part := range parts {
        spl := strings.Split(part, "=")
        if len(spl) != 2 {
            return "", nil, errors.New("What are you doing trying to assign two values to one. Watch your spaces.")
        }
        spl[1], err = checkVARCHARClosed(spl[1])
        if err != nil {
            return "", nil, err
        }
        colVals[spl[0]] = spl[1]
    }

    fmt.Println(colVals)

    return tbName, &colVals, nil
}



func checkVARCHARClosed(s string) (string, error) {
    if s[0] == '\'' {
        if s[len(s)-1] != '\'' {
            return "", errors.New("Expected ' to be closed.")
        }
        return s[1:len(s)-1], nil
    }

    if s[0] == '"' {
        if s[len(s)-1] != '"' {
            return "", errors.New("Expected ' to be closed.")
        }
        return s[1:len(s)-1], nil
    }
    return s, nil
}



