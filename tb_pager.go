package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

// Pager
type Pager struct {
	fileDescriptor *os.File
	fileLength     int64
	numPages       uint32
	pages          [TABLE_MAX_PAGES]*[PAGE_SIZE]byte
}

func (pager *Pager) getPage(pageNum uint32) unsafe.Pointer {
	if pageNum > TABLE_MAX_PAGES {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES)
		os.Exit(EXIT_FAILURE)
	}

	if pager.pages[pageNum] == nil {
		page := &([PAGE_SIZE]byte{})
		numPages := pager.fileLength / int64(PAGE_SIZE)

		if pager.fileLength%int64(PAGE_SIZE) > 0 {
			numPages += 1
		}

		if pageNum <= uint32(numPages) {
			offset, err := pager.fileDescriptor.Seek(int64(pageNum)*int64(PAGE_SIZE), os.SEEK_SET)
			if err != nil || offset == -1 {
				fmt.Printf("Error seeking file: %s\n", err.Error())
				os.Exit(EXIT_FAILURE)
			}

			bytesRead, err := pager.fileDescriptor.Read(page[:])
			if (err != nil && !errors.Is(err, io.EOF)) || bytesRead == -1 {
				fmt.Printf("Error reading file: %s, %d\n", err.Error(), bytesRead)
				os.Exit(EXIT_FAILURE)
			}
		}

		pager.pages[pageNum] = page
		if pageNum >= pager.numPages {
			pager.numPages = pageNum + 1
		}
	}

	return unsafe.Pointer(pager.pages[pageNum])
}

func (pager *Pager) pagerFlush(pageNum int) {
	if pager.pages[pageNum] == nil {
		fmt.Printf("Tried to flush null page\n")
		os.Exit(EXIT_FAILURE)
	}

	offset, err := pager.fileDescriptor.Seek(int64(pageNum)*int64(PAGE_SIZE), os.SEEK_SET)
	if err != nil || offset == -1 {
		fmt.Printf("Error seeking: %s\n", err.Error())
		os.Exit(EXIT_FAILURE)
	}

	bytesWrite, err := pager.fileDescriptor.Write(pager.pages[pageNum][:PAGE_SIZE])
	if err != nil || bytesWrite == -1 {
		fmt.Printf("Error writing: %s\n", err.Error())
		os.Exit(EXIT_FAILURE)
	}
}
