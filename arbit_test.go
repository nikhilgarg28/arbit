package arbit

import (
	"encoding/binary"
	"os"
	"testing"
)

const (
	_test_file = "/tmp/arbit.log"
)

func TestInit(t *testing.T) {
	rb := New(1000, _test_file)
	defer rb.Close()
	// initially all bits should be zero
	for i := uint64(0); i < 1000; i++ {
		if rb.Get(i) {
			t.Errorf("Bit %d is set when it should not be", i)
		}
	}
}

func TestReplication(t *testing.T) {
	lengths := []uint64{0, 1, 1000, 10000000}
	for _, length := range lengths {
		rb := New(length, _test_file)

		for i := uint64(0); i < length; i++ {
			if rb.Get(i) {
				t.Errorf("Bit %d is set when it should not be", i)
			}
			rb.Set(i)
			if !rb.Get(i) {
				t.Errorf("Bit %d is clear when it should not be", i)
			}
			rb.Clear(i)
			if rb.Get(i) {
				t.Errorf("Bit %d is set when it should not be", i)
			}
			rb.Flip(i)
			if !rb.Get(i) {
				t.Errorf("Bit %d is clear when it should not be", i)
			}
		}
		rb.Close()

		// Now read the file and make sure it has all the commands in the
		// correct order
		f, err := os.Open(_test_file)
		if err != nil {
			t.Errorf("Unable to open log file.")
		}
		defer f.Close()
		// we issue total of 1 + 3*length commands, each of which should take 9
		// bytes
		data := make([]byte, 9*(1+3*length))
		n, err := f.Read(data)
		if n != len(data) {
			t.Errorf("Binlog data not of the correct length")
		}

		check := func(index int, code uint8, pos uint64) {
			if data[index] != byte(code) {
				t.Errorf("Code should be '%d' when it is not", code)
			}

			pos_, _ := binary.Uvarint(data[index+1 : index+9])
			if pos_ != pos {
				t.Errorf("Position should be '%d' when it is %d", pos, pos_)
			}
		}
		// check that the first 9 bytes are bitset New command
		check(0, new, length)

		for i, pos := uint64(0), 9; pos < len(data); pos, i = pos+27, i+1 {
			check(pos, set, i)
			check(pos+9, clear, i)
			check(pos+18, flip, i)
		}
	}
}

func TestGetSet(t *testing.T) {
	rb := New(1000, _test_file)
	if rb.Set(0) == true {
		t.Error("Set on bit %d returned true when it should return false", 0)
	}

	if rb.Get(0) == false {
		t.Errorf("Bit %d is not set when it should be", 0)
	}

	if rb.Set(0) == false {
		t.Errorf("Set on bit %d returned false when it should return true", 0)
	}
}

func TestLargeSetGet(t *testing.T) {
	size := uint64(1) << 35
	rb := New(size, _test_file)

	positions := []uint64{0, 1, 10, 1000, 1 << 32, size - 1}
	for _, position := range positions {
		if rb.Get(position) {
			t.Errorf("Bit %d should be false but is true", position)
		}

		rb.Set(position)

		if !rb.Get(position) {
			t.Errorf("Bit %d should be true but is false", position)
		}
	}
}

func TestLength(t *testing.T) {

	sizes := []uint64{0, 1, 10, 1000, 1 << 32, 1 << 33}
	for _, size := range sizes {
		rb := New(size, _test_file)
		if rb.Length() != size {
			t.Errorf("Length should be %d", size)
		}
	}
}

func TestClear(t *testing.T) {
	rb := New(1000, _test_file)
	pos := uint64(1)
	rb.Set(pos)

	if !rb.Get(pos) {
		t.Errorf("Bit %d should be set when it is not", pos)
	}

	if !rb.Clear(pos) {
		t.Error("Clearing bit %d should have returned true", pos)
	}

	if rb.Get(pos) {
		t.Errorf("Bit %d should have been cleared when it is not", pos)
	}
}

func TestFlip(t *testing.T) {
	rb := New(1000, _test_file)
	pos := uint64(1)

	if rb.Get(pos) {
		t.Errorf("Bit %d should be clear when it is not", pos)
	}

	if rb.Flip(pos) {
		t.Error("Flipping bit %d should have returned false, but it did not", pos)
	}

	if !rb.Get(pos) {
		t.Errorf("Bit %d should have been set when it is not", pos)
	}
	if !rb.Flip(pos) {
		t.Error("Flipping bit %d should have returned true, but it did not", pos)
	}

	if rb.Get(pos) {
		t.Errorf("Bit %d should have been clear when it is not", pos)
	}
}
