package utils

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestSelectAndStop(t *testing.T) {
	const (
		recvLen, sendLen, nProd = 50, 10, 10
		timeout                 = time.Second * 2
	)

	chans := make([]chan int, 10)
	inChans := make([]<-chan int, 10)
	for i := range chans {
		chans[i] = make(chan int, 5)
		inChans[i] = chans[i]
	}

	out := make(chan int, 50)
	stop := SelectN(out, inChans...)

	nSent := 0
	for i := 0; i < 5; i++ {
		for i, c := range chans {
			c <- i
			nSent++
		}
	}
	// out should be closed after stop
	// Also tests the remaining messages being drained after stopping
	stop <- Unit{}
	time.Sleep(time.Second)

	timer, nRecvd, done := time.NewTimer(timeout), 0, false
	for !done {
		select {
		case _, ok := <-out:
			if !ok {
				done = true
			} else {
				nRecvd++
			}
		case <-timer.C:
			t.Fatal("Time limit exceeded")
		}
	}
	if nSent != nRecvd {
		t.Fatalf("Expected %d, got %d", nSent, nRecvd)
	}
}

func TestSelectParallel(t *testing.T) {
	const (
		recvLen, sendLen, nProd, nSend = 500, 50, 100, 100
		timeout                        = time.Second * 2
	)

	chans := make([]chan int, nProd)
	inChans := make([]<-chan int, nProd)
	for i := range inChans {
		chans[i] = make(chan int, sendLen)
		inChans[i] = chans[i]
	}

	out := make(chan int, recvLen)
	stop := SelectN(out, inChans...)

	var nSent atomic.Uint32
	start := make(chan int)
	for _, c := range chans {
		go func(c chan<- int) {
			_, _ = <-start
			for i := 1; i <= nSend; i++ {
				c <- i
				nSent.Add(1)
			}
			close(c)
		}(c)
	}
	close(start)

	nRecv, timer, done := uint32(0), time.NewTimer(timeout), false
	for !done {
		select {
		case _, ok := <-out:
			// out should be closed after all other channels are closed
			if !ok {
				done = true
			} else {
				nRecv++
			}
		case <-timer.C:
			t.Error("Time limit exceeded")
			done = true
		}
	}
	if ns := nSent.Load(); ns != nRecv && !t.Failed() {
		t.Errorf("Expected %d messages, got %d", ns, nRecv)
	}

	close(stop)
}

func TestSelectClosesParallel(t *testing.T) {
	const (
		recvLen, sendLen, nProd, nSend = 500, 50, 100, 100
		timeout                        = time.Second * 1
	)

	chans := make([]chan int, nProd)
	inChans := make([]<-chan int, nProd)
	for i := range inChans {
		chans[i] = make(chan int, sendLen)
		inChans[i] = chans[i]
	}

	out := make(chan int, recvLen)
	stop := SelectN(out, inChans...)

	var nSent atomic.Uint32
	start := make(chan int)
	go func(c chan<- int) {
		_, _ = <-start
		for i := 1; i <= nSend; i++ {
			c <- i
			nSent.Add(1)
		}
	}(chans[0])
	for _, c := range chans[1:] {
		go func(c chan<- int) {
			_, _ = <-start
			for i := 1; i <= nSend; i++ {
				c <- i
				nSent.Add(1)
			}
			close(c)
		}(c)
	}
	close(start)

	nRecv, timer, done := uint32(0), time.NewTimer(timeout), false
	for !done {
		select {
		case _, ok := <-out:
			if !ok {
				t.Fatal("Unexpected out chan close")
			} else {
				nRecv++
			}
		case <-timer.C:
			done = true
		}
	}
	if ns := nSent.Load(); ns != nRecv && !t.Failed() {
		t.Errorf("Expected %d messages, got %d", ns, nRecv)
	}

	close(stop)
}
