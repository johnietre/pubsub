package utils

import (
  "testing"
  "time"
)

func TestSelect(t  *testing.T) {
  chans := make([]chan int, 10)
  inChans := make([]<-chan int, 10)
  for i, _ := range chans {
    chans[i] = make(chan int, 5)
    inChans[i] = chans[i]
  }
  out := make(chan int, 50)
  stop := SelectN(out, inChans...)
  for i, c := range chans {
    for j := 0; j < 5; j++ {
      c <- j * 10 + i
    }
  }
  time.Sleep(time.Second)
  for i := 0; i < 50; i++ {
    select {
    case _ = <-out:
    default:
      t.Fatalf("nothing received for %d", i)
    }
  }
  close(stop)
  time.Sleep(time.Second)
}

func TestSelectAndClose(t  *testing.T) {
  chans := make([]chan int, 10)
  inChans := make([]<-chan int, 10)
  for i, _ := range chans {
    chans[i] = make(chan int, 5)
    inChans[i] = chans[i]
  }
  out := make(chan int, 50)
  stop := SelectN(out, inChans...)

  for i, c := range chans {
    c <- i
  }
  stop <- Unit{}
  time.Sleep(time.Second)

  for i := 0; i < 10; i++ {
    select {
    case _ = <-out:
    default:
      t.Fatalf("nothing received for %d", i)
    }
  }

  time.Sleep(time.Second)
}
