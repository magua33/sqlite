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
	LEAF_NODE_NEXT_LEAF_SIZE   = uint32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE
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

const (
	LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
	LEAF_NODE_LEFT_SPLIT_COUNT  = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT
)

// Internal Node Header Layout 内部节点头部布局
const (
	INTERNAL_NODE_NUM_KEYS_SIZE      = uint32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_NUM_KEYS_OFFSET    = COMMON_NODE_HEADER_SIZE
	INTERNAL_NODE_RIGHT_CHILD_SIZE   = uint32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
	INTERNAL_NODE_HEADER_SIZE        = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE
)

// Internal NodeBody Layout 内部节点Body布局
const (
	INTERNAL_NODE_CHILD_SIZE = uint32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_KEY_SIZE   = uint32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_CELL_SIZE  = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
	INTERNAL_NODE_MAX_CELLS  = uint32(3)
)

func leafNodeNextLeaf(node unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(uintptr(node) + uintptr(LEAF_NODE_NEXT_LEAF_OFFSET))
}

func internalNodeNumKeys(node unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(uintptr(node) + uintptr(INTERNAL_NODE_NUM_KEYS_OFFSET))
}

func internalNodeRightChild(node unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(uintptr(node) + uintptr(INTERNAL_NODE_RIGHT_CHILD_OFFSET))
}

func internalNodeCell(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(node) + uintptr(INTERNAL_NODE_HEADER_SIZE) + uintptr(cellNum*INTERNAL_NODE_CELL_SIZE))
}

/*
  在内部节点node中根据子页面索引获得内存坐标
  不能超过该内存节点最大元素个数
  小于childNum 都是在leftChild中存储
  等于时 是rightChild
*/
func internalNodeChild(node unsafe.Pointer, childNum uint32) unsafe.Pointer {
	numKeys := *(*uint32)(internalNodeNumKeys(node))
	if childNum > numKeys {
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys)
		os.Exit(EXIT_FAILURE)
	} else if childNum == numKeys {
		return internalNodeRightChild(node)
	} else {
		return internalNodeCell(node, childNum)
	}
	return nil
}

func internalNodeKey(node unsafe.Pointer, keyNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(internalNodeCell(node, keyNum)) + uintptr(INTERNAL_NODE_CHILD_SIZE))
}

func initializeInternalNode(node unsafe.Pointer) {
	setNodeType(node, NODE_INTERNAL)
	setNodeRoot(node, false)
	*(*uint32)(internalNodeNumKeys(node)) = 0
}

// 遍历通用节点
func getNodeMaxKey(node unsafe.Pointer) uint32 {
	switch getNodeType(node) {
	case NODE_INTERNAL:
		return *(*uint32)(internalNodeKey(node, (*(*uint32)(internalNodeNumKeys(node)))-1))
	case NODE_LEAF:
		return *(*uint32)(leafNodeKey(node, (*(*uint32)(leafNodeNumCells(node)))-1))
	}

	return 0
}

func isNodeRoot(node unsafe.Pointer) bool {
	value := *(*bool)(unsafe.Pointer(uintptr(node) + uintptr(IS_ROOT_OFFSET)))
	return value
}

func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	oldNode := cursor.table.pager.getPage(cursor.pageNum)
	oldMax := getNodeMaxKey(oldNode)

	newPageNum := getUnUsedPageNum(cursor.table.pager)
	newNode := cursor.table.pager.getPage(newPageNum)

	initializeLeafNode(newNode)

	*(*uint32)(nodeParent(newNode)) = *(*uint32)(nodeParent(oldNode))

	*(*uint32)(leafNodeNextLeaf(newNode)) = *(*uint32)(leafNodeNextLeaf(oldNode))
	*(*uint32)(leafNodeNextLeaf(oldNode)) = newPageNum

	for i := int32(LEAF_NODE_MAX_CELLS); i >= 0; i-- {
		var destinationNode unsafe.Pointer
		if i >= int32(LEAF_NODE_LEFT_SPLIT_COUNT) {
			destinationNode = newNode
		} else {
			destinationNode = oldNode
		}

		indexWithinNode := i % int32(LEAF_NODE_LEFT_SPLIT_COUNT)
		destination := leafNodeCell(destinationNode, uint32(indexWithinNode))

		if i == int32(cursor.cellNum) {
			serializeRow(value, leafNodeValue(destinationNode, uint32(indexWithinNode)))
			*(*uint32)(leafNodeKey(destinationNode, uint32(indexWithinNode))) = key
		} else if i > int32(cursor.cellNum) {
			copy((*(*[LEAF_NODE_CELL_SIZE]byte)(destination))[:], (*(*[LEAF_NODE_CELL_SIZE]byte)(leafNodeCell(oldNode, uint32(i-1))))[:])
		} else {
			copy((*(*[LEAF_NODE_CELL_SIZE]byte)(destination))[:], (*(*[LEAF_NODE_CELL_SIZE]byte)(leafNodeCell(oldNode, uint32(i))))[:])
		}
	}

	*(*uint32)(leafNodeNumCells(oldNode)) = LEAF_NODE_LEFT_SPLIT_COUNT
	*(*uint32)(leafNodeNumCells(newNode)) = LEAF_NODE_RIGHT_SPLIT_COUNT

	if isNodeRoot(oldNode) {
		createNewRoot(cursor.table, newPageNum)
		return
	} else {
		parentPageNum := *(*uint32)(nodeParent(oldNode))
		newMax := getNodeMaxKey(oldNode)
		parent := cursor.table.pager.getPage(parentPageNum)

		updateInternalNodeKey(parent, oldMax, newMax)
		internalNodeInsert(cursor.table, parentPageNum, newPageNum)
	}
}

func updateInternalNodeKey(node unsafe.Pointer, oldKey, newKey uint32) {
	oldChildIndex := internalNodeFindChild(node, oldKey)
	*(*uint32)(internalNodeKey(node, oldChildIndex)) = newKey
}

func internalNodeInsert(table *Table, parentPageNum, childPageNum uint32) {
	parent := table.pager.getPage(parentPageNum)
	child := table.pager.getPage(childPageNum)

	childMaxKey := getNodeMaxKey(child)
	index := internalNodeFindChild(parent, childMaxKey)

	originNumKeys := *(*uint32)(internalNodeNumKeys(parent))
	*(*uint32)(internalNodeNumKeys(parent)) = originNumKeys + 1

	if originNumKeys >= INTERNAL_NODE_MAX_CELLS {
		fmt.Printf("Need to implement splitting internal node\n")
		os.Exit(EXIT_FAILURE)
	}

	rightChildPageNum := *(*uint32)(internalNodeRightChild(parent))
	rightChild := table.pager.getPage(rightChildPageNum)

	if childMaxKey > getNodeMaxKey(rightChild) {
		*(*uint32)(internalNodeChild(parent, originNumKeys)) = rightChildPageNum
		*(*uint32)(internalNodeKey(parent, originNumKeys)) = getNodeMaxKey(rightChild)
		*(*uint32)(internalNodeRightChild(parent)) = childPageNum
	} else {
		for i := originNumKeys; i > index; i-- {
			destination := internalNodeCell(parent, i)
			source := internalNodeCell(parent, i-1)
			copy((*(*[INTERNAL_NODE_CELL_SIZE]byte)(destination))[:], (*(*[INTERNAL_NODE_CELL_SIZE]byte)(source))[:])
		}
		*(*uint32)(internalNodeChild(parent, index)) = childPageNum
		*(*uint32)(internalNodeKey(parent, index)) = childMaxKey
	}

}

func nodeParent(node unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(uintptr(node) + uintptr(PARENT_POINTER_OFFSET))
}

func createNewRoot(table *Table, rightChildPageNum uint32) {
	root := table.pager.getPage(table.rootPageNum)
	rightChild := table.pager.getPage(rightChildPageNum)

	leftChildPageNum := getUnUsedPageNum(table.pager)
	leftChild := table.pager.getPage(leftChildPageNum)

	copy((*(*[PAGE_SIZE]byte)(leftChild))[:], (*(*[PAGE_SIZE]byte)(root))[:])
	setNodeRoot(leftChild, false)

	initializeInternalNode(root)
	setNodeRoot(root, true)
	*(*uint32)(internalNodeNumKeys(root)) = 1
	*(*uint32)(internalNodeChild(root, 0)) = leftChildPageNum

	leftChildMaxKey := getNodeMaxKey(leftChild)
	*(*uint32)(internalNodeKey(root, 0)) = leftChildMaxKey
	*(*uint32)(internalNodeRightChild(root)) = rightChildPageNum

	*(*uint32)(nodeParent(leftChild)) = table.rootPageNum
	*(*uint32)(nodeParent(rightChild)) = table.rootPageNum
}

func update_internal_node_key(node unsafe.Pointer, oldKey, newKey uint32) {
	oldChildIndex := internalNodeFindChild(node, oldKey)
	*(*uint32)(internalNodeKey(node, oldChildIndex)) = newKey
}

func setNodeRoot(node unsafe.Pointer, isRoot bool) {
	value := isRoot
	*(*bool)(unsafe.Pointer(uintptr(node) + uintptr(IS_ROOT_OFFSET))) = value
}

func getUnUsedPageNum(pager *Pager) uint32 {
	return pager.numPages
}

func getNodeType(node unsafe.Pointer) NodeType {
	value := *(*uint8)(unsafe.Pointer(uintptr(node) + uintptr(NODE_TYPE_OFFSET)))
	return NodeType(value)
}

func setNodeType(node unsafe.Pointer, typ NodeType) {
	value := uint8(typ)
	*(*uint8)(unsafe.Pointer(uintptr(node) + uintptr(NODE_TYPE_OFFSET))) = value
}

func leafNodeNumCells(node unsafe.Pointer) unsafe.Pointer {
	return (unsafe.Pointer(uintptr(node) + uintptr(LEAF_NODE_NUM_CELLS_OFFSET)))
}

func leafNodeCell(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(node) + uintptr(LEAF_NODE_HEADER_SIZE) + uintptr(cellNum*LEAF_NODE_CELL_SIZE))
}

func leafNodeKey(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(leafNodeCell(node, cellNum)) + uintptr(LEAF_NODE_KEY_OFFSET))

}

func leafNodeValue(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(leafNodeCell(node, cellNum)) + uintptr(LEAF_NODE_VALUE_OFFSET))
}

func initializeLeafNode(node unsafe.Pointer) {
	setNodeType(node, NODE_LEAF)
	setNodeRoot(node, false)
	*(*uint32)(leafNodeNumCells(node)) = 0
	*(*uint32)(leafNodeNextLeaf(node)) = 0
}

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := cursor.table.pager.getPage(cursor.pageNum)

	numCells := *(*uint32)(leafNodeNumCells(node))
	if numCells >= LEAF_NODE_MAX_CELLS {
		leafNodeSplitAndInsert(cursor, key, value)
		return
	}

	if cursor.cellNum < numCells {
		// Make room for new cell
		for i := numCells; i > cursor.cellNum; i-- {
			copy((*(*[LEAF_NODE_CELL_SIZE]byte)(leafNodeCell(node, i)))[:], (*(*[LEAF_NODE_CELL_SIZE]byte)(leafNodeCell(node, i-1)))[:])
		}
	}

	*(*uint32)(leafNodeNumCells(node)) += 1
	*(*uint32)(leafNodeKey(node, cursor.cellNum)) = key
	serializeRow(value, leafNodeValue(node, cursor.cellNum))
}

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Printf("  ")
	}
}

func printTree(pager *Pager, pageNum uint32, indentationLevel uint32) {
	node := pager.getPage(pageNum)
	var numKeys, child uint32

	switch getNodeType(node) {
	case NODE_INTERNAL:
		numKeys = *(*uint32)(internalNodeNumKeys(node))
		indent(indentationLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child = *(*uint32)(internalNodeChild(node, i))
			printTree(pager, child, indentationLevel+1)

			indent(indentationLevel + 1)
			fmt.Printf("- key %d\n", *(*uint32)(internalNodeKey(node, i)))
		}
		child = *(*uint32)(internalNodeRightChild(node))
		printTree(pager, child, indentationLevel+1)
		break
	case NODE_LEAF:
		numKeys = *(*uint32)(leafNodeNumCells(node))
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentationLevel + 1)
			fmt.Printf("- %d\n", *(*uint32)(leafNodeKey(node, i)))
		}
		break
	}
}
