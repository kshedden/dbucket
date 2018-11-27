// GENERATED CODE, DO NOT EDIT

package dbucket

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"time"
)

// ReadFloat64 obtains an array of float64 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadFloat64(name string, stripe int) []float64 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Float64 {
		msg := fmt.Sprintf("Variable '%s' does not have type float64, cannot use ReadFloat64 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []float64
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadFloat32 obtains an array of float32 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadFloat32(name string, stripe int) []float32 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Float32 {
		msg := fmt.Sprintf("Variable '%s' does not have type float32, cannot use ReadFloat32 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []float32
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadUint64 obtains an array of uint64 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadUint64(name string, stripe int) []uint64 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Uint64 {
		msg := fmt.Sprintf("Variable '%s' does not have type uint64, cannot use ReadUint64 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []uint64
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadUint32 obtains an array of uint32 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadUint32(name string, stripe int) []uint32 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Uint32 {
		msg := fmt.Sprintf("Variable '%s' does not have type uint32, cannot use ReadUint32 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []uint32
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadUint16 obtains an array of uint16 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadUint16(name string, stripe int) []uint16 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Uint16 {
		msg := fmt.Sprintf("Variable '%s' does not have type uint16, cannot use ReadUint16 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []uint16
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadUint8 obtains an array of uint8 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadUint8(name string, stripe int) []uint8 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Uint8 {
		msg := fmt.Sprintf("Variable '%s' does not have type uint8, cannot use ReadUint8 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []uint8
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadInt64 obtains an array of int64 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadInt64(name string, stripe int) []int64 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Int64 {
		msg := fmt.Sprintf("Variable '%s' does not have type int64, cannot use ReadInt64 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []int64
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadInt32 obtains an array of int32 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadInt32(name string, stripe int) []int32 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Int32 {
		msg := fmt.Sprintf("Variable '%s' does not have type int32, cannot use ReadInt32 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []int32
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadInt16 obtains an array of int16 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadInt16(name string, stripe int) []int16 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Int16 {
		msg := fmt.Sprintf("Variable '%s' does not have type int16, cannot use ReadInt16 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []int16
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadInt8 obtains an array of int8 values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadInt8(name string, stripe int) []int8 {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Int8 {
		msg := fmt.Sprintf("Variable '%s' does not have type int8, cannot use ReadInt8 to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []int8
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// ReadTime obtains an array of time.Time values stored for variable 'name' in
// the given stripe.
func (r *Reader) ReadTime(name string, stripe int) []time.Time {

	p, ok := r.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable '%s' not found.\n", name)
		panic(msg)
	}

	if r.info.Dtypes[p] != Time {
		msg := fmt.Sprintf("Variable '%s' does not have type time.Time, cannot use ReadTime to access it.\n", name)
		panic(msg)
	}

	if stripe >= len(r.info.Offsets[0]) {
		m := len(r.info.Offsets[0])
		msg := fmt.Sprintf("The dbucket contains %d stripes, cannot access stripe location %d\n", m, stripe)
		panic(msg)
	}

	_, err := r.f.Seek(int64(r.info.Offsets[p][stripe]), io.SeekStart)
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(r.r)
	if err != nil {
		panic(err)
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)

	var x []time.Time
	err = dec.Decode(&x)
	if err != nil {
		panic(err)
	}

	return x
}

// NewFloat6464 creates a new variable in the Dbucket with the given name.  It
// will hold float6464 data values.
func (b *Builder) NewFloat64(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Float64)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewFloat3264 creates a new variable in the Dbucket with the given name.  It
// will hold float3264 data values.
func (b *Builder) NewFloat32(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Float32)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewUint6464 creates a new variable in the Dbucket with the given name.  It
// will hold uint6464 data values.
func (b *Builder) NewUint64(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Uint64)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewUint3264 creates a new variable in the Dbucket with the given name.  It
// will hold uint3264 data values.
func (b *Builder) NewUint32(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Uint32)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewUint1664 creates a new variable in the Dbucket with the given name.  It
// will hold uint1664 data values.
func (b *Builder) NewUint16(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Uint16)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewUint864 creates a new variable in the Dbucket with the given name.  It
// will hold uint864 data values.
func (b *Builder) NewUint8(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Uint8)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewInt6464 creates a new variable in the Dbucket with the given name.  It
// will hold int6464 data values.
func (b *Builder) NewInt64(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Int64)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewInt3264 creates a new variable in the Dbucket with the given name.  It
// will hold int3264 data values.
func (b *Builder) NewInt32(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Int32)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewInt1664 creates a new variable in the Dbucket with the given name.  It
// will hold int1664 data values.
func (b *Builder) NewInt16(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Int16)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewInt864 creates a new variable in the Dbucket with the given name.  It
// will hold int864 data values.
func (b *Builder) NewInt8(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Int8)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// NewTime64 creates a new variable in the Dbucket with the given name.  It
// will hold time.Time64 data values.
func (b *Builder) NewTime(name string) {

	if _, ok := b.info.Pos[name]; !ok {
		p := len(b.info.Pos)
		b.info.Names = append(b.info.Names, name)
		b.info.Pos[name] = p
		b.info.Dtypes = append(b.info.Dtypes, Time)
		var x, y []uint64
		b.info.Offsets = append(b.info.Offsets, x)
		b.info.Sizes = append(b.info.Sizes, y)
	} else {
		msg := fmt.Sprintf("Name '%s' already exists\n", name)
		panic(msg)
	}
}

// AppendFloat64 adds a stripe of data for the given variable.  The variable
// must contain float64 data values.
func (b *Builder) AppendFloat64(name string, data []float64) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendFloat32 adds a stripe of data for the given variable.  The variable
// must contain float32 data values.
func (b *Builder) AppendFloat32(name string, data []float32) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendUint64 adds a stripe of data for the given variable.  The variable
// must contain uint64 data values.
func (b *Builder) AppendUint64(name string, data []uint64) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendUint32 adds a stripe of data for the given variable.  The variable
// must contain uint32 data values.
func (b *Builder) AppendUint32(name string, data []uint32) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendUint16 adds a stripe of data for the given variable.  The variable
// must contain uint16 data values.
func (b *Builder) AppendUint16(name string, data []uint16) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendUint8 adds a stripe of data for the given variable.  The variable
// must contain uint8 data values.
func (b *Builder) AppendUint8(name string, data []uint8) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendInt64 adds a stripe of data for the given variable.  The variable
// must contain int64 data values.
func (b *Builder) AppendInt64(name string, data []int64) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendInt32 adds a stripe of data for the given variable.  The variable
// must contain int32 data values.
func (b *Builder) AppendInt32(name string, data []int32) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendInt16 adds a stripe of data for the given variable.  The variable
// must contain int16 data values.
func (b *Builder) AppendInt16(name string, data []int16) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendInt8 adds a stripe of data for the given variable.  The variable
// must contain int8 data values.
func (b *Builder) AppendInt8(name string, data []int8) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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

// AppendTime adds a stripe of data for the given variable.  The variable
// must contain time.Time data values.
func (b *Builder) AppendTime(name string, data []time.Time) {

	p, ok := b.info.Pos[name]
	if !ok {
		msg := fmt.Sprintf("Variable %s not found.", name)
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

	start := b.currentPosition()
	gz := gzip.NewWriter(b.w)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
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
