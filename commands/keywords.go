package commands

import (
)

type CommandKeyWord uint8

const (
    SELECT CommandKeyWord = iota
    INSERT
    DELETE
    UPDATE
    NONE
)


var CommandKeyWordMap = map[string] CommandKeyWord {
    "SELECT": SELECT,
    "INSERT": INSERT,
    "DELETE": DELETE,
    "UPDATE": UPDATE,
}


func GetCommandKeyWord (s string) CommandKeyWord {
    cmd, ok := CommandKeyWordMap[s]
    if !ok {
        return NONE
    }
    return cmd
}

