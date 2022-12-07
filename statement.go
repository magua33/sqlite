package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_NEGATIVE_ID
	PREPARE_STRING_TOO_LONG
	PREPARE_UNRECOGNIZED_STATEMENT
	PREPARE_SYNTAX_ERROR
)

// MetaCommandResult 命令执行结果
type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if string(inputBuffer.buffer) == ".exit" {
		inputBuffer.closeInputBuffer()
		table.dbClose()
		os.Exit(EXIT_SUCCESS)
	} else if string(inputBuffer.buffer) == ".constants" {
		fmt.Printf("Constants:\n")
		printConstants()
		return META_COMMAND_SUCCESS
	} else if string(inputBuffer.buffer) == ".btree" {
		fmt.Printf("Tree:\n")
		printTree(table.pager, 0, 0)
		return META_COMMAND_SUCCESS
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Statement struct {
	typ         StatementType
	rowToInsert Row
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	inputStr := string(inputBuffer.buffer)

	if len(inputStr) >= 6 && inputStr[:6] == "insert" {
		return prepareInsert(inputBuffer, statement)
	}

	if inputStr == "select" {
		statement.typ = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.typ {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		return executeSelect(statement, table)
	default:
		return EXECUTE_SUCCESS
	}
}

// @Insert
func prepareInsert(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	statement.typ = STATEMENT_INSERT

	inputStr := *(*string)(unsafe.Pointer(&(inputBuffer.buffer)))

	inputs := strings.Split(inputStr, " ")
	if len(inputs) < 4 {
		return PREPARE_SYNTAX_ERROR
	}

	// keyword := inputs[0]
	idString := inputs[1]
	username := inputs[2]
	email := inputs[3]
	if idString == "" || username == "" || email == "" {
		return PREPARE_SYNTAX_ERROR
	}

	id, err := strconv.Atoi(idString)
	if err != nil {
		fmt.Printf("Error atoi idString:%s\n", err.Error())
		return PREPARE_SYNTAX_ERROR
	}
	if id < 0 {
		return PREPARE_NEGATIVE_ID
	}

	if len(username) > COLUMN_USERNAME_SIZE {
		return PREPARE_STRING_TOO_LONG
	}

	if len(email) > COLUMN_EMAIL_SIZE {
		return PREPARE_STRING_TOO_LONG
	}

	statement.rowToInsert.id = uint32(id)
	copy(statement.rowToInsert.username[:], []byte(username))
	copy(statement.rowToInsert.email[:], []byte(email))

	return PREPARE_SUCCESS
}
