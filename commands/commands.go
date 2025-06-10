package commands

import (
	"fmt"
	"strings"
	"time"
    "errors"

	"github.com/MalikL2005/SeliaDB-II/types"
)

func CommandWrapper (fullQuery string, db *types.Database_t) error {
    if db == nil {
        return errors.New("No database selected. Use \\use to connect to an existing database or CREATE DATABASE to create a new one.")
    }
    defer MeasureTime()()
    for query := range strings.SplitSeq(fullQuery, ";"){
        if query == "" {
            break
        }
        err := ParseQuery(query, db)
        if err != nil {
            return err
        }
    }
    return nil
}

func MeasureTime () func() {
    start := time.Now()
    return func(){
        fmt.Printf("-- Query took %v\n", time.Since(start))
    }
}


