package main

import (
	"fmt"
	"os"
)

func main() {
	argc := os.Args
	if len(argc) < 2 {
		fmt.Printf("Must supply a database filename.\n")
		os.Exit(EXIT_FAILURE)
	}

	fileName := argc[1]
	table := dbOpen(fileName)

	inputBuffer := newInputBuffer()

	for {
		printPrompt()
		inputBuffer.readInput()

		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer, table) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", string(inputBuffer.buffer))
				continue
			}

		}

		if string(inputBuffer.buffer) == "test" {
			test(inputBuffer, table)
		} else {
			var statement Statement
			switch prepareStatement(inputBuffer, &statement) {
			case PREPARE_SUCCESS:
				break
			case PREPARE_NEGATIVE_ID:
				fmt.Printf("ID must be positive.\n")
				continue
			case PREPARE_STRING_TOO_LONG:
				fmt.Printf("String is too long.\n")
				continue
			case PREPARE_SYNTAX_ERROR:
				fmt.Printf("Syntax error. could not parse statement.\n")
				continue
			case PREPARE_UNRECOGNIZED_STATEMENT:
				fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
				continue
			}

			switch executeStatement(&statement, table) {
			case EXECUTE_SUCCESS:
				fmt.Printf("Executed.\n")
				break
			case EXECUTE_TABLE_FULL:
				fmt.Printf("Error: Table full.\n")
				break
			}
		}

	}
}

const (
	EXIT_FAILURE = 1
	EXIT_SUCCESS = 0
)

func printPrompt() {
	fmt.Printf("db > ")
}

/*
func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}
*/

// @Test
func test(inputBuffer *InputBuffer, table *Table) {

	for i := 1; i < 1401; i++ {
		inputStr := fmt.Sprintf("insert %d user%d user%d@qq.com", i, i, i)

		var statement Statement
		inputBuffer.buffer = []byte(inputStr)
		switch prepareStatement2(inputBuffer, &statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_NEGATIVE_ID:
			fmt.Printf("ID must be positive.\n")
		case PREPARE_STRING_TOO_LONG:
			fmt.Printf("String is too long.\n")
		case PREPARE_SYNTAX_ERROR:
			fmt.Printf("Syntax error. could not parse statement.\n")
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
		}

		switch executeStatement(&statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Printf("Executed.\n")
			break
		case EXECUTE_TABLE_FULL:
			fmt.Printf("Error: Table full.\n")
			break
		}
	}

}

func prepareStatement2(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
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
