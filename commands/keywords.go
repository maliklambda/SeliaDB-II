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


var commandKeyWordMap = map[string] CommandKeyWord {
    "SELECT": SELECT,
    "INSERT": INSERT,
    "DELETE": DELETE,
    "UPDATE": UPDATE,
}


func GetCommandKeyWord (s string) CommandKeyWord {
    cmd, ok := commandKeyWordMap[s]
    if !ok {
        return NONE
    }
    return cmd
}

