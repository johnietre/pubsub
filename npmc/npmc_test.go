package npmc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSimpleAndClose(t *testing.T) {
	const (
		nSend, subLen = 50, 100
		timeout       = time.Second * 1
	)
	ntwk := NewNetwork[int]()

	pubClient, subClient := ntwk.NewClient(), ntwk.NewClient()
	defer pubClient.Close()
	defer subClient.Close()

	pubChan, ok := pubClient.NewPub("test")
	if !ok {
		t.Fatal("Failed to create pub chan")
	}
	subChan, ok := subClient.NewSub("test", subLen)
	if !ok {
		t.Fatal("Failed to sub to chan")
	}

	nSent := 0
	for i := 0; i < nSend; i++ {
		pubChan.Pub(i)
		nSent++
	}
	if !pubChan.Close() {
		t.Fatal("Failed to close pub chan")
	}

	ch, nRecvd, timer, done := subChan.Chan(), 0, time.NewTimer(timeout), false
	for !done {
		select {
		case _, ok := <-ch:
			if !ok {
				done = true
			} else {
				nRecvd++
			}
		case <-timer.C:
			t.Fatal("Exceeded time limit")
		}
	}
	if nRecvd != nSent {
		t.Fatalf("Expected %d, got %d", nSent, nRecvd)
	}
}

func TestSubCurrentConcurrent(t *testing.T) {
	const (
		nPubs, nSendEach = 1000, 100
		nSubs, nChanLen  = 6000, 300
	)
	var wg sync.WaitGroup
	start := make(chan bool)
	ntwk := NewNetwork[int]()
	var nSent atomic.Uint32
	// Make pubs
	for i := 0; i < nPubs; i++ {
		id := i + 1
		pubClient := ntwk.NewClient()
		chName := fmt.Sprint(id)
		_, ok := pubClient.NewPub(chName)
		if !ok {
			t.Errorf("Could not create pub channel for id %d", id)
			return
		}
		wg.Add(1)
		go func(client *Client[int], id int) {
			defer client.Close()
			ch := client.GetPub(chName)
			if ch == nil {
				t.Errorf("Could not get pub channel for id %d", id)
				wg.Done()
				return
			}
			wg.Done()

			_, _ = <-start
			for i := 0; i < nSendEach; i++ {
				if !ch.Pub(i) {
					t.Errorf("Pub %d failed to pub", id)
					return
				}
				nSent.Add(1)
			}
		}(pubClient, id)
	}
	wg.Wait()
	if t.Failed() {
		t.Fatal("Failed preconditions")
	}

	// Make subs
	timer := newTimer(time.Second * 5)
	var nRecvd atomic.Uint32
	for i := 0; i < nSubs; i++ {
		wg.Add(1)
		go func(id int) {
			subClient := ntwk.NewClientSubCurrent(id%2 == 0, nChanLen)
			if id%600 == 0 {
				println(id)
			}
			mc := subClient.MergeAllSubs(nChanLen)
			wg.Done()
			_, _ = <-start
			wg.Add(1)
			defer wg.Done()
			done := false
			for !done {
				select {
				case _, ok := <-mc.Chan():
					if !ok {
						done = true
					} else {
						nRecvd.Add(1)
					}
				case _, _ = <-timer.c:
					t.Errorf("Sub %d exceeded time limit", id)
					done = true
				}
			}
		}(i + 1)
	}
	wg.Wait()
	close(start)
	timer.Start()

	time.Sleep(time.Millisecond * 500)
	wg.Wait()
	ns, nr := nSent.Load(), nRecvd.Load()
	if nr%ns != 0 {
		t.Fatalf("Expected nRecvd (%d) to be multiple of nSent (%d)", nr, ns)
	}
	nr /= nSubs
	if nr != ns {
		t.Fatalf("Expected %d, got %d", ns, nr)
	}
}

type Timer struct {
	started atomic.Bool
	c       chan struct{}
	dur     time.Duration
}

func (t *Timer) Start() {
	if !t.started.Swap(true) {
		go func() {
			time.Sleep(t.dur)
			close(t.c)
		}()
	}
}

func newTimer(dur time.Duration) *Timer {
	return &Timer{
		c:   make(chan struct{}),
		dur: dur,
	}
}
