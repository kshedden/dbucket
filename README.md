 __dbucket__ is a simple data container file format, and a Go libary
 for reading and writing the files.  It can be used in Go programs
 that manipulate rectangular arrays of data.  A typical use-case would
 be to maintain a large (e.g. 200GB) data array on disk in the dbucket
 format.  The data can then be processed "stripe by stripe" using a Go
 program.  It is essentially a very simple "write once read many
 times" database, aiming primarily for the use-case of working with
 "data frames".

Dbucket has somewhat similar goals as [Apache
 Orc](https://orc.apache.org/), but is considerably simpler, and
 specific to Go, as it uses
 [gobs](https://blog.golang.org/gobs-of-data) and some other
 Go-specific data formats.

A dbucket file holds a data array with n rows and p columns of data.
Each column of the array has a data type, and all the values within a
column have the same type.  Dbucket currently supports 13 data types
(4 signed integer types, 4 unsigned integer types, 2 numeric float
types, strings, time values, and bit fields).

The array is partitioned into "stripes" of contiguous rows.  On-disk,
the data for one stripe of a column are stored column-wise, i.e. it is
a "columnar storage" format.  The data can be accessed in a random
access manner, retrieving the data for one variable in one stripe with
a single function call.  The data are returned as a native Go array,
e.g. []float32.

On-disk, the primary data and meta-data are serialized as Go gobs,
which are compressed using gzip.  Strings are dictionary-coded, and it
is possible to retrieve either the original string values, or the
uint64 codes and the mapping from strings to codes.
