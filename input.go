package main

import (
	"bufio"
	"fmt"
	"os"
)

type InputBuffer struct {
	buffer       []byte
	bufferLength int
	inputLength  int
}

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer:       nil,
		bufferLength: 0,
		inputLength:  0,
	}
}

func (inputBuffer *InputBuffer) readInput() {
	var err error
	inputBuffer.buffer, _, err = bufio.NewReader(os.Stdin).ReadLine()
	if err != nil {
		fmt.Printf("Error reading input: %s\n", err.Error())
		os.Exit(EXIT_FAILURE)
	}

	if len(inputBuffer.buffer) <= 0 {
		fmt.Printf("Error reading input\n")
		os.Exit(EXIT_FAILURE)
	}
}

func (inputBuffer *InputBuffer) closeInputBuffer() {
	inputBuffer = nil
}
