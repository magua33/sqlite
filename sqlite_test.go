package main

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestInsert(t *testing.T) {
	fileName := "mydb.db"
	table := dbOpen(fileName)

	for i := 0; i < 34; i++ {
		var statement Statement
		statement.typ = STATEMENT_INSERT
		statement.rowToInsert.id = uint32(i)
		copy(statement.rowToInsert.username[:], []byte(fmt.Sprintf("user%d", i)))
		copy(statement.rowToInsert.email[:], []byte(fmt.Sprintf("person%d@qq.com", i)))

		executeInsert(&statement, table)
	}

	table.dbClose()
}

func BenchmarkWriteBySwap(b *testing.B) {
	page := [10240]byte{}

	const (
		IDSize       int = 4
		UsernameSize int = 32
		EmailSize    int = 255
		RowSize      int = 291
		PageSize     int = 10240

		IDOffset       int = 0
		UsernameOffset int = 4
		EmailOffset    int = 36
	)

	// id := uint32(7)
	username := "seven"
	email := "seven7@gmail.com"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 34; j++ {
			rowOffset := j * RowSize

			var ub = [UsernameSize]byte{}
			var eb = [EmailSize]byte{}

			copy(ub[:], []byte(username))
			copy(eb[:], []byte(email))

			// *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(IDOffset))) = id
			*(*[UsernameSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(UsernameOffset))) = ub
			*(*[EmailSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(EmailOffset))) = eb
		}

		x := PageSize / RowSize
		for k := 0; k < x; k++ {
			idx := k
			rowOffset := idx * RowSize
			// *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(IDOffset))) = 0
			*(*[UsernameSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(UsernameOffset))) = [UsernameSize]byte{}
			*(*[EmailSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(EmailOffset))) = [EmailSize]byte{}
		}
	}

}

func BenchmarkWriteByCopy(b *testing.B) {
	page := [10240]byte{}

	const (
		IDSize       int = 4
		UsernameSize int = 32
		EmailSize    int = 255
		RowSize      int = 291
		PageSize     int = 10240

		IDOffset       int = 0
		UsernameOffset int = 4
		EmailOffset    int = 36
	)

	// id := uint32(7)
	username := "seven"
	email := "seven7@gmail.com"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 34; j++ {
			rowOffset := j * RowSize

			// idb := [IDSize]byte{}
			// bit := 8
			// for i := 0; i < IDSize; i++ {
			// 	idb[i] = uint8(id >> (i * bit))
			// 	idb[i] = uint8(id >> (i * bit))
			// 	idb[i] = uint8(id >> (i * bit))
			// 	idb[i] = uint8(id >> (i * bit))
			// }
			// copy(page[rowOffset+IDOffset:], idb[:])
			copy(page[rowOffset+UsernameOffset:], []byte(username))
			copy(page[rowOffset+EmailOffset:], []byte(email))
		}

		x := PageSize / RowSize
		for k := 0; k < x; k++ {
			idx := k
			rowOffset := idx * RowSize

			// id := page[rowOffset+IDOffset : rowOffset+IDOffset+IDSize]
			// _ = uint32(id[3]) | uint32(id[2])<<8 | uint32(id[1])<<16 | uint32(id[0])<<24
			_ = page[rowOffset+UsernameOffset : rowOffset+UsernameOffset+UsernameSize]
			_ = page[rowOffset+EmailOffset : rowOffset+EmailOffset+EmailSize]
		}
	}

}
