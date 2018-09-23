/*
Package dbucket provides a simple serialization format for a rectangular array containing
fixed width or string values.  Each column of the array has a type, and the elements within
the column must all have the same type.  The array is partitioned into row-contiguous stripes.
The contents of one stripe of one column can be accessed in a random-access manner.  These
values are returned as one dimensional Go arrays with native types.

On-disk, the data are stored column-wise by stripe. The values are compressed using gzip compression.
String values are dictionary-compressed.
*/

package dbucket

// A fixed width data type that can be the storage format of the data in one column
// of the array.
type DType uint8

const (
	Uint8 DType = iota
	Uint16
	Uint32
	Uint64
	Int8
	Int16
	Int32
	Int64
	Float32
	Float64
	String
	Time
	Bit
)

type info struct {

	// The variable names, in the order of the columns of the array
	Names []string

	// Offsets[i][j] contains the offset position for stripe j of variable i
	Offsets [][]uint64

	// Sizes[i][j] contains the number of bytes of compressed data used to store
	// the data for stripe j of variable i
	Sizes [][]uint64

	// Rows[i] contains the number of rows in stripe i
	Rows []uint64

	// Dtypes contains the dtype of each column
	Dtypes []DType

	// A codeset is a mapping from strings to unique integers.  Each column is coded
	// using the codeset given by this value.
	CodeSets []string

	// Dictionaries for columns compressed with dictionary compression.
	Dicts map[string]map[string]uint64

	// Reversed dictionaries mapping integer codes to string values
	DictsRev map[string]map[uint64]string

	// Pos is a map from variable names to column positions in the array
	Pos map[string]int
}

// newInfo creates and initializes an info struct.
func newInfo() *info {
	inf := new(info)
	inf.Pos = make(map[string]int)
	inf.Dicts = make(map[string]map[string]uint64)
	inf.DictsRev = make(map[string]map[uint64]string)
	return inf
}
