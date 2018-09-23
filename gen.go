// +build ignore

package main

import (
	"bytes"
	"go/format"
	"os"
	"text/template"
)

type Dtype struct {
	TypeU string
	TypeL string
}

var (
	Ftypes = []Dtype{
		Dtype{"Float64", "float64"},
		Dtype{"Float32", "float32"},
		Dtype{"Uint64", "uint64"},
		Dtype{"Uint32", "uint32"},
		Dtype{"Uint16", "uint16"},
		Dtype{"Uint8", "uint8"},
		Dtype{"Int64", "int64"},
		Dtype{"Int32", "int32"},
		Dtype{"Int16", "int16"},
		Dtype{"Int8", "int8"},
		Dtype{"Time", "time.Time"},
	}
)

func main() {

	tmpl, err := template.ParseFiles("defs.template")
	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, Ftypes)
	if err != nil {
		panic(err)
	}

	var p []byte
	p, err = format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	out, err := os.Create("defs_gen.go")
	if err != nil {
		panic(err)
	}
	out.WriteString("// GENERATED CODE, DO NOT EDIT\n\n")
	_, err = out.Write(p)
	if err != nil {
		panic(err)
	}
	out.Close()
}
