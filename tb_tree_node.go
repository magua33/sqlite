package main

import (
	"fmt"
	"os"
	"unsafe"
)

// Node Header Format
type NodeType uint32

const (
	NODE_INTERNAL NodeType = iota
	NODE_LEAF
)

// Common Node Header Layout
const (
	NODE_TYPE_SIZE   = uint32(unsafe.Sizeof(uint8(0)))
	NODE_TYPE_OFFSET = uint32(0)
	IS_ROOT_SIZE     = uint32(unsafe.Sizeof(uint8(0)))
	IS_ROOT_OFFSET   = uint32(NODE_TYPE_SIZE)

	PARENT_POINTER_SIZE     = uint32(unsafe.Sizeof(uint32(0)))
	PARENT_POINTER_OFFSET   = IS_ROOT_OFFSET + IS_ROOT_SIZE
	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

// Leaf Node Format
const (
	LEAF_NODE_NUM_CELLS_SIZE   = uint32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
)

// Leaf Node Body Layout
const (
	LEAF_NODE_KEY_SIZE        = uint32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_KEY_OFFSET      = 0
	LEAF_NODE_VALUE_SIZE      = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET    = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
	LEAF_NODE_CELL_SIZE       = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS       = +LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
)

func leafNodeNumCells(node unsafe.Pointer) unsafe.Pointer {
	return (unsafe.Pointer(uintptr(node) + uintptr(LEAF_NODE_NUM_CELLS_OFFSET)))
}

func leafNodeCell(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(node) + uintptr(cellNum*LEAF_NODE_CELL_SIZE))
}

func leafNodeKey(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(leafNodeCell(node, cellNum)) + uintptr(LEAF_NODE_KEY_OFFSET))

}

func leafNodeValue(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(leafNodeCell(node, cellNum)) + uintptr(LEAF_NODE_VALUE_OFFSET))
}

func initializeLeafNode(node unsafe.Pointer) {
	*(*uint32)(leafNodeNumCells(node)) = 0
}

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := cursor.table.pager.getPage(cursor.pageNum)

	numCells := *(*uint32)(leafNodeNumCells(node))
	if numCells >= LEAF_NODE_MAX_CELLS {
		// Node full
		fmt.Printf("Need to implement splitting a leaf node.\n")
		os.Exit(EXIT_FAILURE)
	}

	if cursor.cellNum < numCells {
		// Make room for new cell
		for i := numCells; i > cursor.cellNum; i-- {
			copy((*(*[LEAF_NODE_CELL_SIZE]byte)(leafNodeCell(node, i)))[:], (*(*[LEAF_NODE_CELL_SIZE]byte)(leafNodeCell(node, i-1)))[:])
		}
	}

	*(*uint32)(leafNodeNumCells(node)) += 1
	*(*uint32)(leafNodeKey(node, cursor.cellNum)) = key
	serializeRow(value, unsafe.Pointer(leafNodeValue(node, cursor.cellNum)))
}

func printLeafNode(node unsafe.Pointer) {
	numCells := *(*uint32)(leafNodeNumCells(node))
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		key := *(*uint32)(leafNodeKey(node, i))
		fmt.Printf("  - %d : %d\n", i, key)
	}
}
