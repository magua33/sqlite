package main

import "unsafe"

type Cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
	endOfTable bool
}

func tableStart(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.pageNum = table.rootPageNum
	cursor.cellNum = 0

	rootNode := table.pager.getPage(table.rootPageNum)
	numCells := *(*uint32)(leafNodeNumCells(rootNode))
	cursor.endOfTable = numCells == 0

	return cursor
}

func tableEnd(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.pageNum = table.rootPageNum

	rootNode := table.pager.getPage(table.rootPageNum)
	numCells := *(*uint32)(leafNodeNumCells(rootNode))
	cursor.cellNum = numCells

	cursor.endOfTable = true

	return cursor
}

func (cursor *Cursor) cursorValue() unsafe.Pointer {
	node := cursor.table.pager.getPage(cursor.pageNum)
	rowNum := cursor.cellNum
	return unsafe.Pointer(leafNodeValue(node, rowNum))
}

func (cursor *Cursor) cursorAdvance() {
	pageNum := cursor.pageNum
	node := cursor.table.pager.getPage(pageNum)

	cursor.cellNum += 1

	nodeCell := *(*uint32)(leafNodeNumCells(node))

	if cursor.cellNum >= nodeCell {
		cursor.endOfTable = true
	}
}
