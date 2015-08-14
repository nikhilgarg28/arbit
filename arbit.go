package arbit

import (
	"bufio"
	"encoding/binary"
	"github.com/nikhilgarg28/bitset"
	"os"
	"time"
)

const bufsize = 1 << 20

// we flush the channel atleast every 1 second. Our target is to 1M
// writes/second, so the channel should be able to hold around ~1M messages at
// any point of time.
const channelsize = 1 << 20

const (
	new = 1 + iota
	set
	clear
	flip
)

type command struct {
	code  uint8
	index uint64
}

type replication struct {
	binlogfile string
	writes     chan command
	stop       chan bool
	stopped    chan bool
}

type Arbit struct {
	b *bitset.Bitset
	r replication
}

func handle(err error) {
	if err != nil {
		panic(err)
	}
}

var long = make([]byte, 8)

func writeCommand(w *bufio.Writer, cmd *command) {
	w.WriteByte(cmd.code)
	binary.PutUvarint(long, cmd.index)
	w.Write(long)
}

func flush(writes *chan command, w *bufio.Writer, force bool) uint {
	num := uint(0)
loop:
	for {
		select {
		case write, ok := <-*writes:
			if ok {
				writeCommand(w, &write)
				num += 1
			} else {
				break loop
			}

		default:
			break loop
		}
	}

	if force {
		handle(w.Flush())
	}
	return num
}

func drain(writes *chan command, w *bufio.Writer, force bool) {
	for {
		num := flush(writes, w, force)
		if num <= 0 {
			return
		}
	}
}

func (r *replication) replicate() {
	f, err := os.Create(r.binlogfile)
	handle(err)

	defer func() {
		handle(f.Close())
	}()

	// make a buffered writer
	w := bufio.NewWriterSize(f, bufsize)

	ticker := time.Tick(time.Second)

	for {
		select {
		case <-ticker:
			flush(&r.writes, w, true)
		case <-r.stop:
			close(r.stop)
			close(r.writes)
			drain(&r.writes, w, true)
			close(r.stopped)
			return
		default:
			flush(&r.writes, w, false)
		}
	}
}

func New(length uint64, binlogfile string) *Arbit {
	b := bitset.New(length)
	r := replication{
		binlogfile,
		make(chan command, channelsize),
		make(chan bool),
		make(chan bool),
	}

	r.writes <- command{new, length}
	go r.replicate()

	return &Arbit{b, r}
}

func (rb *Arbit) Close() {
	replication := rb.r
	replication.stop <- true
	<-replication.stopped
}

func (rb *Arbit) Length() uint64 {
	return rb.b.Length()
}

func (rb *Arbit) Get(pos uint64) bool {
	return rb.b.Get(pos)
}

func (rb *Arbit) Set(pos uint64) bool {
	ret := rb.b.Set(pos)
	rb.r.writes <- command{set, pos}
	return ret
}

func (rb *Arbit) Clear(pos uint64) bool {
	ret := rb.b.Clear(pos)
	rb.r.writes <- command{clear, pos}
	return ret
}

func (rb *Arbit) Flip(pos uint64) bool {
	ret := rb.b.Flip(pos)
	rb.r.writes <- command{flip, pos}
	return ret
}
