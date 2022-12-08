package main

import (
	"encoding/binary"
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
		IDSize       int = 8
		UsernameSize int = 32
		EmailSize    int = 251
		RowSize      int = 291
		PageSize     int = 10240

		IDOffset       int = 0
		UsernameOffset int = 8
		EmailOffset    int = 40
	)

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

			*(*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(IDOffset))) = uint64(i)
			*(*[UsernameSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(UsernameOffset))) = ub
			*(*[EmailSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(EmailOffset))) = eb
		}

		x := PageSize / RowSize
		for k := 0; k < x; k++ {
			idx := k
			rowOffset := idx * RowSize
			*(*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(IDOffset))) = 0
			*(*[UsernameSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(UsernameOffset))) = [UsernameSize]byte{}
			*(*[EmailSize]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&page)) + uintptr(rowOffset) + uintptr(EmailOffset))) = [EmailSize]byte{}
		}
	}

}

func BenchmarkWriteByCopy(b *testing.B) {
	page := [10240]byte{}

	const (
		IDSize       int = 8
		UsernameSize int = 32
		EmailSize    int = 251
		RowSize      int = 291
		PageSize     int = 10240

		IDOffset       int = 0
		UsernameOffset int = 8
		EmailOffset    int = 40
	)

	username := "seven"
	email := "seven7@gmail.com"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 34; j++ {
			rowOffset := j * RowSize

			idb := [IDSize]byte{}
			binary.BigEndian.PutUint64(idb[:], uint64(i))
			copy(page[rowOffset+IDOffset:], idb[:])
			copy(page[rowOffset+UsernameOffset:], []byte(username))
			copy(page[rowOffset+EmailOffset:], []byte(email))
		}

		x := PageSize / RowSize
		for k := 0; k < x; k++ {
			idx := k
			rowOffset := idx * RowSize

			id := page[rowOffset+IDOffset : rowOffset+IDOffset+IDSize]
			_ = binary.LittleEndian.Uint64(id)
			_ = page[rowOffset+UsernameOffset : rowOffset+UsernameOffset+UsernameSize]
			_ = page[rowOffset+EmailOffset : rowOffset+EmailOffset+EmailSize]
		}
	}

}
