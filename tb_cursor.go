package main

import "unsafe"

type Cursor struct {
	table      *Table
	rowNum     uint32
	endOfTable bool // Indicates a position one past the last element
}

func tableStart(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.rowNum = 0
	cursor.endOfTable = table.numRows == 0
	return cursor
}

func tableEnd(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.rowNum = table.numRows
	cursor.endOfTable = true
	return cursor
}

func (cursor *Cursor) cursorValue() unsafe.Pointer {
	rowNum := cursor.rowNum
	pageNum := rowNum / ROWS_PER_PAGE

	page := cursor.table.pager.getPage(pageNum)

	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return unsafe.Pointer(uintptr(page) + uintptr(byteOffset))
}

func (cursor *Cursor) cursorAdvance() {
	cursor.rowNum += 1

	if cursor.rowNum >= cursor.table.numRows {
		cursor.endOfTable = true
	}
}
