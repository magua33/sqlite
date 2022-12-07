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
	cursor := tableFind(table, 0)

	node := table.pager.getPage(cursor.pageNum)
	numCells := *(*uint32)(leafNodeNumCells(node))
	cursor.endOfTable = (numCells == 0)

	return cursor
}

func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.rootPageNum
	rootNode := table.pager.getPage(rootPageNum)

	if getNodeType(rootNode) == NODE_LEAF {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		return internalNodeFind(table, rootPageNum, key)
	}
}

func internalNodeFindChild(node unsafe.Pointer, key uint32) uint32 {
	numKeys := *(*uint32)(internalNodeNumKeys(node))

	min := uint32(0)
	max := numKeys
	for max != min {
		index := (min + max) / 2
		indexKey := *(*uint32)(internalNodeKey(node, index))
		if indexKey >= key {
			max = index
		} else {
			min = index + 1
		}
	}

	return min
}

func internalNodeFind(table *Table, pageNum, key uint32) *Cursor {
	node := table.pager.getPage(pageNum)

	childIndex := internalNodeFindChild(node, key)
	childNum := *(*uint32)(internalNodeChild(node, childIndex))

	child := table.pager.getPage(childNum)

	switch getNodeType(child) {
	case NODE_INTERNAL:
		return internalNodeFind(table, childNum, key)
	case NODE_LEAF:
		return leafNodeFind(table, childNum, key)
	default:
		fmt.Println("Error node type!")
		os.Exit(EXIT_FAILURE)
		return nil
	}
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := table.pager.getPage(pageNum)
	numCells := *(*uint32)(leafNodeNumCells(node))

	cursor := &Cursor{}
	cursor.table = table
	cursor.pageNum = pageNum

	minIndex := uint32(0)
	onePastMaxIndex := numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		indexKey := *(*uint32)(leafNodeKey(node, index))

		if key == indexKey {
			cursor.cellNum = index
			return cursor
		}

		if key < indexKey {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cursor.cellNum = minIndex
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
		nextPageNum := *(*uint32)(leafNodeNextLeaf(node))
		if nextPageNum == 0 {
			cursor.endOfTable = true
		} else {
			cursor.pageNum = nextPageNum
			cursor.cellNum = 0
		}
	}
}
