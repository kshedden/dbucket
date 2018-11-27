[Godoc documentation](https://godoc.org/github.com/kshedden/dbucket) for dbucket

__dbucket__ is a simple data container file format, and a Go libary
for reading and writing dbucket files.  It can be used in Go programs
that manipulate rectangular arrays of data.  A typical use-case would
be to maintain a large (e.g. 100's of GB) data array on disk in the dbucket
format.  The data can then be processed "stripe by stripe" using a Go
program.  The stripes are used to limit the amount of data that are read
into main memory at once.  dbucket is essentially a very simple "write once
read many times" database, aiming primarily for the use-case of working with
"data frames".

Dbucket has somewhat similar goals as [Apache
Orc](https://orc.apache.org/), but is considerably simpler, and is
specific to Go, as it uses
[gobs](https://blog.golang.org/gobs-of-data) and some other
Go-specific data formats.  It is obviously somewhat less performant
than mature projects such as Orc.

A dbucket file holds a data array with n rows and p columns of data.
Each column of the array has a data type, and all the values within a
column have the same type.  Dbucket currently supports 13 data types
(4 signed integer types, 4 unsigned integer types, 2 numeric float
types, strings, time values, and bit fields).

The array is partitioned into "stripes" of contiguous rows.  On-disk,
the data for one stripe of a column are stored column-wise, i.e. it is
a "columnar storage" format.  The data can be accessed in a random
access manner, retrieving the data for one variable in one stripe with
a single function call.  The data are returned as a typed slice of
go values, e.g. []float32 or []time.Time (the exception to this is
bit arrays, which are returned as a [bitarray.BitArray](https://godoc.org/github.com/Workiva/go-datastructures/bitarray)
value).

On-disk, the data and meta-data are serialized as Go gobs,
which are compressed using gzip (compression is invisible to the user, but
results in a smaller file size).  Strings are dictionary-coded, and it
is possible to retrieve either the original string values, or the
uint64 codes and the mapping from strings to codes.

Dbucket is not currently "zero copy", but could possibly be made so without
a great deal of effort.

__Example__

The following example creates a dbucket file on disk named "data.dbk".

```
f, _ := os.Create("data.dbk")
b := dbucket.NewFileBuilder(f)

// Create a new column with name "x", the data will be float64's.
b.NewFloat64("x")

// "animals" defines a code set (mapping from strings to uint64 codes)
// that can be shared by several columns.
b.NewString("y", "animals")

// Write a stripe containing three rows
b.StartStripe()
b.AppendFloat64("x", []float64{34, 1, 67})
b.AppendString("y", []string{"cat", "dog", "mouse"})

// Write a stripe containing two rows
b.StartStripe()
b.AppendFloat64("x", []float64{-89, 13})
b.AppendString("y", []string{"goat", "horse"})

b.Close()
f.Close()
```

The next example reads data from the dbucket file created above.

```
f, _ := os.Open("data.dbk")
r := dbucket.NewFileReader(f)

// Read the first stripe
x := r.ReadFloat64("x", 0)
y := r.ReadString("y", 0)
```
