package main

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

const (
	ID_SIZE       uint32 = uint32(unsafe.Sizeof(Row{}.id))
	USERNAME_SIZE uint32 = uint32(unsafe.Sizeof(Row{}.username))
	EMAIL_SIZE    uint32 = uint32(unsafe.Sizeof(Row{}.email))

	ID_OFFSET       uint32 = 0
	USERNAME_OFFSET uint32 = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    uint32 = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        uint32 = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

const (
	PAGE_SIZE       uint32 = 4096
	TABLE_MAX_PAGES uint32 = 100
	ROWS_PER_PAGE   uint32 = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  uint32 = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
)

type Row struct {
	id       uint32
	username [COLUMN_USERNAME_SIZE]byte
	email    [COLUMN_EMAIL_SIZE]byte
}

// Table
type Table struct {
	pager   *Pager
	numRows uint32
}

func dbOpen(filename string) *Table {
	table := &Table{}
	table.pagerOpen(filename)

	numRows := uint32(table.pager.fileLength) / ROW_SIZE
	table.numRows = numRows

	return table
}

func (table *Table) dbClose() {
	pager := table.pager
	numFullPages := table.numRows / ROWS_PER_PAGE

	for i := 0; i < int(numFullPages); i++ {
		if pager.pages[i] == nil {
			continue
		}

		pager.pagerFlush(i, PAGE_SIZE)
		pager.pages[i] = nil
	}

	// There may be a partial page to write to the end of the file
	// This should not be needed after we switch to a B-tree
	numAdditionalRows := table.numRows % ROWS_PER_PAGE
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			pager.pagerFlush(int(pageNum), numAdditionalRows*ROW_SIZE)
			pager.pages[pageNum] = nil
		}
	}

	err := table.pager.fileDescriptor.Close()
	if err != nil {
		fmt.Printf("Error closing db file. %s\n", err.Error())
		os.Exit(EXIT_FAILURE)
	}

	for i := 0; i < int(TABLE_MAX_PAGES); i++ {
		table.pager.pages[i] = nil
	}
	table.pager = nil
	table = nil
}

func (table *Table) pagerOpen(filename string) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, syscall.S_IWUSR|syscall.S_IRUSR)
	if err != nil {
		fmt.Printf("Unable to open file:%s\n", err.Error())
		os.Exit(EXIT_FAILURE)
		return
	}

	fileLength, err := file.Seek(0, os.SEEK_END)
	if err != nil || fileLength == -1 {
		fmt.Printf("Error seel file:%s\n", err.Error())
		os.Exit(EXIT_FAILURE)
		return
	}

	pager := &Pager{}
	pager.fileDescriptor = file
	pager.fileLength = fileLength

	table.pager = pager

	for i := uint32(0); i < TABLE_MAX_PAGES; i++ {
		table.pager.pages[i] = nil
	}
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.numRows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	rowToInsert := &(statement.rowToInsert)
	cursor := tableEnd(table)

	serializeRow(rowToInsert, cursor.cursorValue())
	table.numRows += 1
	cursor = nil

	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	cursor := tableStart(table)

	var row Row
	for !cursor.endOfTable {
		deserializeRow(cursor.cursorValue(), &row)
		printRow(&row)
		cursor.cursorAdvance()
	}

	cursor = nil

	return EXECUTE_SUCCESS
}

func serializeRow(source *Row, destination unsafe.Pointer) {
	*(*uint32)(unsafe.Pointer(uintptr(destination) + uintptr(ID_OFFSET))) = source.id
	*(*([COLUMN_USERNAME_SIZE]byte))(unsafe.Pointer(uintptr(destination) + uintptr(USERNAME_OFFSET))) = source.username
	*(*([COLUMN_EMAIL_SIZE]byte))(unsafe.Pointer(uintptr(destination) + uintptr(EMAIL_OFFSET))) = source.email
}

func deserializeRow(source unsafe.Pointer, destination *Row) {
	destination.id = *(*uint32)(unsafe.Pointer(uintptr(source) + uintptr(ID_OFFSET)))
	destination.username = *(*[COLUMN_USERNAME_SIZE]byte)(unsafe.Pointer(uintptr(source) + uintptr(USERNAME_OFFSET)))
	destination.email = *(*[COLUMN_EMAIL_SIZE]byte)(unsafe.Pointer(uintptr(source) + uintptr(EMAIL_OFFSET)))
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.id, row.username, row.email)
}
