package main

import (
	"fmt"
	"os"
	"unsafe"
)

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

// func tableEnd(table *Table) *Cursor {
// 	cursor := &Cursor{}
// 	cursor.table = table
// 	cursor.pageNum = table.rootPageNum

// 	rootNode := table.pager.getPage(table.rootPageNum)
// 	numCells := *(*uint32)(leafNodeNumCells(rootNode))
// 	cursor.cellNum = numCells

// 	cursor.endOfTable = true

// 	return cursor
// }

func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.rootPageNum
	rootNode := table.pager.getPage(rootPageNum)

	if getNodeType(rootNode) == NODE_LEAF {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		fmt.Printf("Need to implement searching an internal node\n")
		os.Exit(EXIT_FAILURE)
	}
	return nil
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := table.pager.getPage(pageNum)
	numCells := *(*uint32)(leafNodeNumCells(node))

	cursor := &Cursor{}
	cursor.table = table
	cursor.pageNum = pageNum

	min := uint32(0)
	max := numCells
	for max != min {
		index := min + (max-min)/2
		indexKey := *(*uint32)(leafNodeKey(node, key))

		if key == indexKey {
			cursor.cellNum = index
			return cursor
		}

		if key < indexKey {
			max = index
		} else {
			min = index + 1
		}
	}

	cursor.cellNum = min
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
