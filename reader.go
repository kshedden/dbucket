package dbucket

//go:generate go run gen.go

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/Workiva/go-datastructures/bitarray"
)

type Reader struct {

	// Used to read data from the file
	r io.Reader

	// Used to move around in the file
	f io.ReadSeeker

	// Information about the Dbucket being written
	info *info
}

func (r *Reader) readFooter() {

	// Read the offset to the footer
	_, err := r.f.Seek(-11, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	var footpos uint64
	err = binary.Read(r.f, binary.LittleEndian, &footpos)
	if err != nil {
		panic(err)
	}

	// Check for "magic number at end of file"
	mg := make([]byte, 3)
	_, err = r.r.Read(mg)
	if err != nil {
		panic(err)
	}
	if string(mg) != "DBK" {
		msg := "The file does not appear to be a dbucket file, or has been truncated."
		panic(msg)
	}

	_, err = r.f.Seek(int64(footpos), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var dicts map[string]map[string]uint64
	err = dec.Decode(&dicts)
	if err != nil {
		panic(err)
	}

	info := new(info)
	err = dec.Decode(info)
	if err != nil {
		panic(err)
	}

	r.info = info
	r.info.Dicts = dicts
	r.info.DictsRev = make(map[string]map[uint64]string)
}

// NumStripes returns the number of stripes in the dbucket.
func (r *Reader) NumStripes() int {
	if len(r.info.Names) == 0 {
		return 0
	}
	return len(r.info.Names[0])
}

// Dicts returns a map from variable names whose data type is string to code maps.  Each
// code map associates a unique value to a unique integer code.
func (r *Reader) Dicts() map[string]map[string]uint64 {

	return r.info.Dicts
}

// NewFileReader creates a new Reader foro obtaining data from
// the given Dbucket file.
func NewFileReader(f *os.File) *Reader {

	r := &Reader{
		r: f,
		f: f,
	}

	r.readFooter()
	return r
}

// Rows returns the number of rows in the given stripe.
func (r *Reader) Rows(stripe int) int {

	return int(r.info.Rows[stripe])
}

func reverse(x map[string]uint64) map[uint64]string {

	y := make(map[uint64]string)

	for k, v := range x {
		y[v] = k
	}

	return y
}

// ReadString obtains an array of string values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadString(name string, stripe int) []string {

	p := r.info.Pos[name]

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	fpos := int64(r.info.Offsets[p][stripe])
	_, err := r.f.Seek(fpos, io.SeekStart)
	if err != nil {
		panic(err)
	}

	s, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	br := bufio.NewReader(s)

	n := int(r.info.Rows[stripe])
	x := make([]string, n)

	cx := r.info.CodeSets[p]
	dr := r.info.DictsRev[cx]
	if dr == nil {
		dr = reverse(r.info.Dicts[cx])
		r.info.DictsRev[cx] = dr
	}

	for i := 0; i < n; i++ {
		u, err := binary.ReadUvarint(br)
		if err != nil {
			panic(err)
		}

		v, ok := dr[u]
		if !ok {
			msg := fmt.Sprintf("Key not found in dictionary for variable '%s'\n", name)
			panic(msg)
		}
		x[i] = v
	}

	return x
}

// ReadStringCodes obtains an array of string values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadStringCodes(name string, stripe int) []uint64 {

	p := r.info.Pos[name]

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	fpos := int64(r.info.Offsets[p][stripe])
	_, err := r.f.Seek(fpos, io.SeekStart)
	if err != nil {
		panic(err)
	}

	s, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	br := bufio.NewReader(s)

	n := int(r.info.Rows[stripe])
	u := make([]uint64, n)
	for i := 0; i < n; i++ {
		u[i], err = binary.ReadUvarint(br)
		if err != nil {
			panic(err)
		}
	}

	return u
}

// ReadBit obtains an array of bit values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadBit(name string, stripe int) bitarray.BitArray {

	p := r.info.Pos[name]

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	fpos := int64(r.info.Offsets[p][stripe])
	_, err := r.f.Seek(fpos, io.SeekStart)
	if err != nil {
		panic(err)
	}

	// Read compressed data
	bt := make([]byte, r.info.Sizes[p][stripe])
	_, err = r.r.Read(bt)
	if err != nil {
		panic(err)
	}

	// Get uncompressed (but still marshalled) data
	gz, err := gzip.NewReader(bytes.NewReader(bt))
	if err != nil {
		panic(err)
	}
	bx, err := ioutil.ReadAll(gz)
	if err != nil {
		panic(err)
	}
	gz.Close()

	x, err := bitarray.Unmarshal(bx)
	if err != nil {
		panic(err)
	}

	return x
}
