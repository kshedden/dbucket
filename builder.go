package dbucket

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"

	"github.com/Workiva/go-datastructures/bitarray"
)

// Builder supports construction of a Dbucket from a striped array.
type Builder struct {

	// Information about the Dbucket being constructed
	info *info

	// Used for writing data
	w io.WriteCloser

	// Used for moving around
	file io.WriteSeeker

	// Within each stripe, we need to know if any variables have been written yet
	firstVar bool

	// Indicates which variables have been written for the current stripe
	written []bool
}

// NewFileBuilder creates a new Builder object that will write to the provided
// file (which should be a newly created file).
func NewFileBuilder(file *os.File) *Builder {

	// Magic number
	io.WriteString(file, "DBK")

	bld := &Builder{
		info: newInfo(),
		w:    file,
		file: file,
	}

	return bld
}

// StartStripe must be called before any data is written to a given stripe.
func (b *Builder) StartStripe() {

	if len(b.info.Names) == 0 {
		panic("No variables have been created, cannot call StartStripe yet\n")
	}

	if len(b.written) != len(b.info.Names) {
		b.written = make([]bool, len(b.info.Names))
	} else {
		for i := range b.written {
			b.written[i] = false
		}
	}

	b.firstVar = true
}

func (b *Builder) currentPosition() uint64 {

	pos, err := b.file.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}

	return uint64(pos)
}

// NewString creates a new variable in the Dbucket with the given name.  It
// will hold dictionary-encoded string values.  The codeset argument specifies
// a label for the codeset used to dictionary-code the string values.  Two
// columns with the same codeset value will be encoded using a common set
// of codes.
func (b *Builder) NewString(name string, codeset string) {

	if _, ok := b.info.Pos[name]; !ok {

		// Column position of the variable
		p := len(b.info.Pos)

		// Set up the codeset and dictionary
		if codeset == "" {
			codeset = name
		}
		for len(b.info.CodeSets) <= p {
			b.info.CodeSets = append(b.info.CodeSets, "")
		}
		b.info.CodeSets[p] = codeset
		b.info.Dicts[codeset] = make(map[string]uint64)

		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, String)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewBit creates a new variable in the Dbucket with the given name.  It
// will hold binary (bit array) values.
func (b *Builder) NewBit(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Bit)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// AppendString adds a stripe for the given variable.  The variable
// must contain string data values.
func (b *Builder) AppendString(name string, data []string) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.\n", name)
		panic(msg)
	}

	if b.info.Dtypes[p] != String {
		msg := fmt.Sprintf("Variable %s does not hold string values.\n", name)
		panic(msg)
	}

	if b.firstVar {
		b.info.Rows = append(b.info.Rows, uint64(len(data)))
	} else {
		m := b.info.Rows[len(b.info.Rows)-1]
		if int(m) != len(data) {
			msg := fmt.Sprintf("Invalid number of values in variable %s, expected %d, found %d.",
				name, m, len(data))
			panic(msg)
		}
	}

	buf := make([]byte, binary.MaxVarintLen64)

	start := b.currentPosition()

	gz := gzip.NewWriter(b.w)
	cx := b.info.CodeSets[p]
	if cx == "" {
		msg := fmt.Sprintf("CodeSet not set for string variable %s.\n", name)
		panic(msg)
	}
	di := b.info.Dicts[cx]
	if di == nil {
		di = make(map[string]uint64)
		b.info.Dicts[cx] = di
	}

	for _, x := range data {

		u, ok := di[x]
		if !ok {
			u = uint64(len(di))
			di[x] = u
		}

		n := binary.PutUvarint(buf, u)
		_, err := gz.Write(buf[0:n])
		if err != nil {
			panic(err)
		}
	}

	gz.Close()

	end := b.currentPosition()

	b.info.Offsets[p] = append(b.info.Offsets[p], uint64(start))
	b.info.Sizes[p] = append(b.info.Sizes[p], uint64(end-start))

	b.written[p] = true
	b.firstVar = false
}

// AppendBit adds a stripe for the given variable.  The variable
// must contain bit data values.
func (b *Builder) AppendBit(name string, data bitarray.BitArray, size int) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.\n", name)
		panic(msg)
	}

	if b.info.Dtypes[p] != Bit {
		msg := fmt.Sprintf("Variable %s does not hold bit values.\n", name)
		panic(msg)
	}

	if b.firstVar {
		b.info.Rows = append(b.info.Rows, uint64(size))
	} else {
		m := b.info.Rows[len(b.info.Rows)-1]
		if int(m) != size {
			msg := fmt.Sprintf("Invalid number of values in variable %s, expected %d, found %d.",
				name, m, size)
			panic(msg)
		}
	}

	start := b.currentPosition()

	gz := gzip.NewWriter(b.w)

	bt, err := bitarray.Marshal(data)
	if err != nil {
		panic(err)
	}

	_, err = gz.Write(bt)
	if err != nil {
		panic(err)
	}

	gz.Close()

	end := b.currentPosition()

	b.info.Offsets[p] = append(b.info.Offsets[p], uint64(start))
	b.info.Sizes[p] = append(b.info.Sizes[p], uint64(end-start))

	b.written[p] = true
	b.firstVar = false
}

// Close writes the Dbucket footer and closes the file.
func (b *Builder) Close() {

	footstart := b.currentPosition()

	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(b.info.Dicts)
	if err != nil {
		panic(err)
	}

	err = enc.Encode(b.info)
	if err != nil {
		panic(err)
	}

	gz.Close()

	// Write the position of the info struct
	err = binary.Write(b.w, binary.LittleEndian, footstart)
	if err != nil {
		panic(err)
	}

	// Write a magic number at the very end of the file
	io.WriteString(b.w, "DBK")

	b.w.Close()
}
