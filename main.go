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
			case EXECUTE_DUPLICATE_KEY:
				fmt.Printf("Error: Duplicate key.\n")
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

func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}

// @Test
var testCount = 0

var testArr = []string{
	"insert 18 user18 person18@example.com", "insert 7 user7 person7@example.com", "insert 10 user10 person10@example.com",
	"insert 29 user29 person29@example.com", "insert 23 user23 person23@example.com", "insert 4 user4 person4@example.com",
	"insert 14 user14 person14@example.com", "insert 30 user30 person30@example.com", "insert 15 user15 person15@example.com",
	"insert 26 user26 person26@example.com", "insert 22 user22 person22@example.com", "insert 19 user19 person19@example.com",
	"insert 2 user2 person2@example.com", "insert 1 user1 person1@example.com", "insert 21 user21 person21@example.com",
	"insert 11 user11 person11@example.com", "insert 6 user6 person6@example.com", "insert 20 user20 person20@example.com",
	"insert 5 user5 person5@example.com", "insert 8 user8 person8@example.com", "insert 13 user13 person13@example.com",
	"insert 9 user9 person9@example.com", "insert 3 user3 person3@example.com", "insert 12 user12 person12@example.com",
	"insert 27 user27 person27@example.com", "insert 17 user17 person17@example.com", "insert 16 user16 person16@example.com",
	"insert 24 user24 person24@example.com", "insert 25 user25 person25@example.com", "insert 28 user28 person28@example.com",
}

func test(inputBuffer *InputBuffer, table *Table) {
	testCount++

	for i := 0; i < len(testArr); i++ {
		inputStr := testArr[i]

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
		case EXECUTE_DUPLICATE_KEY:
			fmt.Printf("Error: Duplicate key.\n")
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
