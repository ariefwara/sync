package core

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ReadInt32 membaca int32 (4 byte, little-endian) dari reader.
func ReadInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// WriteInt32 menulis int32 (4 byte, little-endian) ke writer.
func WriteInt32(w io.Writer, v int32) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// ReadExact membaca n byte persis dari reader.
func ReadExact(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// WriteExact menulis data ke writer.
func WriteExact(w io.Writer, data []byte) error {
	_, err := w.Write(data)
	return err
}

// ReadMsg membaca pesan lengkap: [4-byte length][payload].
func ReadMsg(r io.Reader) ([]byte, error) {
	length, err := ReadInt32(r)
	if err != nil {
		return nil, fmt.Errorf("read msg length: %w", err)
	}
	return ReadExact(r, int(length))
}

// WriteMsg menulis pesan lengkap: [4-byte length][payload].
func WriteMsg(w io.Writer, data []byte) error {
	if err := WriteInt32(w, int32(len(data))); err != nil {
		return fmt.Errorf("write msg length: %w", err)
	}
	return WriteExact(w, data)
}
