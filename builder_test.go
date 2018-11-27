package dbucket

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Workiva/go-datastructures/bitarray"
	"gonum.org/v1/gonum/floats"
)

const (
	numstripe  int = 10
	stripesize int = 100
)

func TestWriteReadFloat64(t *testing.T) {

	f, err := os.Create("tmp.dbk")
	if err != nil {
		panic(err)
	}

	b := NewFileBuilder(f)

	b.NewFloat64("x")
	b.NewFloat64("y")

	var wantX, wantY [][]float64

	for st := 0; st < numstripe; st++ {

		b.StartStripe()

		var x, y []float64
		for i := 0; i < stripesize+10*st; i++ {
			x = append(x, float64(10*st+2*i))
			y = append(y, float64(10*st+2*i+1))
		}

		b.AppendFloat64("x", x)
		b.AppendFloat64("y", y)

		wantX = append(wantX, x)
		wantY = append(wantY, y)
	}

	b.Close()

	f, err = os.Open("tmp.dbk")
	if err != nil {
		panic(err)
	}

	r := NewFileReader(f)
	names := r.Names()
	if len(names) != 2 || names[0] != "x" || names[1] != "y" {
		t.Fail()
	}
	for st := 0; st < numstripe; st++ {
		x := r.ReadFloat64("x", st)
		y := r.ReadFloat64("y", st)

		if !(floats.Equal(x, wantX[st]) && floats.Equal(y, wantY[st])) {
			t.Fail()
		}
	}
}

func TestWriteReadInt16(t *testing.T) {

	f, err := os.Create("tmp.dbk")
	if err != nil {
		panic(err)
	}

	b := NewFileBuilder(f)

	b.NewInt16("x")

	var wantX [][]int16

	for st := 0; st < numstripe; st++ {

		b.StartStripe()

		var x []int16
		for i := 0; i < stripesize+2*st+1; i++ {
			x = append(x, int16(10*st-2*i))
		}

		b.AppendInt16("x", x)

		wantX = append(wantX, x)
	}

	b.Close()

	f, err = os.Open("tmp.dbk")
	if err != nil {
		panic(err)
	}

	r := NewFileReader(f)
	for st := 0; st < numstripe; st++ {
		x := r.ReadInt16("x", st)
		for i := range x {
			if x[i] != wantX[st][i] {
				t.Fail()
			}
		}
	}
}

func TestWriteReadString(t *testing.T) {

	f, err := os.Create("tmp.dbk")
	if err != nil {
		panic(err)
	}

	b := NewFileBuilder(f)

	b.NewUint8("x")
	b.NewString("y", "")

	var wantX [][]uint8
	var wantY [][]string

	for st := 0; st < numstripe; st++ {

		b.StartStripe()

		var x []uint8
		for i := 0; i < stripesize+3*st+11; i++ {
			x = append(x, uint8(10*st-2*i))
		}

		var y []string
		for i := 0; i < stripesize+3*st+11; i++ {
			y = append(y, fmt.Sprintf(">>%d<<", 13*st-3*i))
		}

		b.AppendUint8("x", x)
		b.AppendString("y", y)

		wantX = append(wantX, x)
		wantY = append(wantY, y)
	}

	b.Close()

	f, err = os.Open("tmp.dbk")
	if err != nil {
		panic(err)
	}

	r := NewFileReader(f)
	for st := 0; st < numstripe; st++ {
		x := r.ReadUint8("x", st)
		for i := range x {
			if x[i] != wantX[st][i] {
				t.Fail()
			}
		}
		y := r.ReadString("y", st)
		for i := range y {
			if y[i] != wantY[st][i] {
				t.Fail()
			}
		}
	}
}

func TestWriteReadTime(t *testing.T) {

	f, err := os.Create("tmp.dbk")
	if err != nil {
		panic(err)
	}

	b := NewFileBuilder(f)

	b.NewFloat32("x")
	b.NewTime("y")

	var wantX [][]float32
	var wantY [][]time.Time

	for st := 0; st < numstripe; st++ {

		b.StartStripe()

		var x []float32
		for i := 0; i < stripesize+4*st+1; i++ {
			x = append(x, float32(10*st-2*i))
		}

		var y []time.Time
		for i := 0; i < stripesize+4*st+1; i++ {
			y = append(y, time.Now().Add(time.Hour*time.Duration(13*st-3*i)))
		}

		b.AppendFloat32("x", x)
		b.AppendTime("y", y)

		wantX = append(wantX, x)
		wantY = append(wantY, y)
	}

	b.Close()

	f, err = os.Open("tmp.dbk")
	if err != nil {
		panic(err)
	}

	r := NewFileReader(f)
	for st := 0; st < numstripe; st++ {
		x := r.ReadFloat32("x", st)
		for i := range x {
			if x[i] != wantX[st][i] {
				t.Fail()
			}
		}
		y := r.ReadTime("y", st)
		for i := range y {
			if !y[i].Equal(wantY[st][i]) {
				t.Fail()
			}
		}
	}
}

func TestWriteReadBit(t *testing.T) {

	f, err := os.Create("tmp.dbk")
	if err != nil {
		panic(err)
	}

	b := NewFileBuilder(f)

	b.NewBit("x")
	b.NewInt32("y")

	var wantX []bitarray.BitArray
	var wantY [][]int32

	for st := 0; st < numstripe; st++ {

		b.StartStripe()

		x := bitarray.NewBitArray(uint64(stripesize + 5*st + 1))
		for i := 0; i < stripesize+5*st+1; i++ {
			if i%3 == 0 {
				x.SetBit(uint64(i))
			}
		}

		var y []int32
		for i := 0; i < stripesize+5*st+1; i++ {
			y = append(y, int32(13*st-3*i))
		}

		b.AppendBit("x", x, stripesize+5*st+1)
		b.AppendInt32("y", y)

		wantX = append(wantX, x)
		wantY = append(wantY, y)
	}

	b.Close()

	f, err = os.Open("tmp.dbk")
	if err != nil {
		panic(err)
	}

	r := NewFileReader(f)
	for st := 0; st < numstripe; st++ {
		x := r.ReadBit("x", st)
		if !x.Equals(wantX[st]) {
			t.Fail()
		}
		y := r.ReadInt32("y", st)
		for i := range y {
			if y[i] != wantY[st][i] {
				t.Fail()
			}
		}
	}
}
