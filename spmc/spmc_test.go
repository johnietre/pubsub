package spmc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnietre/pubsub/utils"
)

func TestSimpleAndClose(t *testing.T) {
  const (
    nSend, subLen = 50, 100
    timeout = time.Second * 5
  )
  ntwk := NewNetwork[int]()

  pubClient, subClient := ntwk.NewClient(0), ntwk.NewClient(subLen)
  defer pubClient.Close()
  defer subClient.Close()

  pubChan, err := pubClient.NewPub("test", true)
  if err != nil {
    t.Fatal("Failed to create pub chan: ", err)
  }
  if _, err := subClient.Sub("test"); err != nil {
    t.Fatal("Failed to sub to chan")
  }

  nSent := 0
  for i := 0; i < nSend; i++ {
    pubChan.Pub(i)
    nSent++
  }
  if err := pubChan.Close(); err != nil {
    t.Fatal("Failed to close pub chan: ", err)
  }

  ch, nRecvd, timer, done := subClient.Chan(), 0, time.NewTimer(timeout), false
  for !done {
    select {
    case msg, ok := <-ch:
      if msg.IsClose() {
        done = true
      } else if !ok {
        t.Fatal("Channel unexpectedly closed")
      } else {
        nRecvd++
      }
    case <-timer.C:
      t.Fatalf("Exceeded time limit, received %d of %d", nSent, nRecvd)
    }
  }
  if nRecvd != nSent {
    t.Fatalf("Expected %d, got %d", nSent, nRecvd)
  }
}

func TestPubSubSimple(t *testing.T) {
  ntwk := NewNetwork[int]()
  pubClient := ntwk.NewClient(0)
  defer pubClient.Close()
  subClient := ntwk.NewClient(100)
  defer subClient.Close()

  var err error

  // Sub Channel1 (fail)
  t.Run("SubChannel1 (fail)", func(t *testing.T) {
    if ch, err := subClient.Sub("1"); err != ErrChanNotExist {
      t.Fatalf(`Expected err %v, got %v`, ErrChanNotExist, err)
    } else if ch != nil {
      t.Fatal("Sub returned non-nil channel for nonexistent channel")
    }
  })

  // Create Channel1
  var ch1 PubChannel[int]
  t.Run("CreateChannel1", func(t *testing.T) {
    ch1, err = pubClient.NewPub("1", false)
    if err != nil {
      t.Fatalf("Unexpected error creating channel: %v", err)
    } else if ch1 == nil {
      t.Fatalf("NewPub returned nil channel")
    }
  })

  // Sub Channel1
  t.Run("SubChannel1", func(t *testing.T) {
    if ch, err := subClient.Sub("1"); err != nil {
      t.Fatalf(`Unexpected subbing error: %v`, err)
    } else if ch == nil {
      t.Fatal("Sub returned nil channel")
    }
  })

  // Pub Channel1
  t.Run("PubChannel1", func(t *testing.T) {
    if err := ch1.Pub(1); err != nil {
      t.Fatalf("Unexpected error: %v", err)
    }
    if ch, err := pubClient.Pub(ch1.Name(), 1); err != nil {
      t.Fatalf("Unexpected error: %v", err)
    } else if ch.Name() != ch1.Name() {
      if ch == nil {
        t.Fatal("Client.Pub returned nil channel")
      } else {
        t.Fatalf(
          "Expected Client.Pub to return channel %s, got %s",
          ch1.Name(), ch.Name(),
        )
      }
    }
  })

  // Create Channel2
  var ch2 PubChannel[int]
  t.Run("CreateChannel2", func(t *testing.T) {
    ch2, err = pubClient.NewPub("2", false)
    if err != nil {
      t.Fatalf("Unexpected error creating channel: %v", err)
    } else if ch2 == nil {
      t.Fatalf("NewPub returned nil channel")
    }
  })

  // Sub Channel2
  var sch2 SubChannel[int]
  t.Run("SubChannel2", func(t *testing.T) {
    sch2, err = subClient.Sub(ch2.Name())
    if err != nil {
      t.Fatalf("Unexpected error subbing: %v", err)
    } else if sch2 == nil {
      t.Fatal("Sub returned nil channel")
    }
    if ch, err := subClient.GetSub(sch2.Name()); err != nil {
      t.Fatalf("Unexpected error getting sub: %v", err)
    } else if ch.Name() != sch2.Name() {
      t.Fatalf("Expected channel %s, got %s", sch2.Name(), ch.Name())
    }
  })

  // Pub Channel2
  t.Run("PubChannel2", func(t *testing.T) {
    if err := ch2.Pub(2); err != nil {
      t.Fatalf("Unexpected error: %v", err)
    }
    if ch, err := pubClient.Pub(ch2.Name(), 2); err != nil {
      t.Fatalf("Unexpected error: %v", err)
    } else if ch.Name() != ch2.Name() {
      if ch == nil {
        t.Fatal("Client.Pub returned nil channel")
      } else {
        t.Fatalf(
          "Expected Client.Pub to return channel %s, got %s",
          ch2.Name(), ch.Name(),
        )
      }
    }
  })

  t.Run("RecvChannel1", func(t *testing.T) {
    // Recv Channel1 (first)
    if msg, ok := <-subClient.Chan(); !ok {
      t.Fatal("subClient chan unexpectedly closed")
    } else if msg.IsClose() {
      t.Fatal("Received Close message")
    } else if name := msg.Channel().Name(); name != ch1.Name() {
      t.Fatalf("Expected msg from channel %s, got from %s", ch1.Name(), name)
    } else if msg.Value() != 1 {
      t.Fatalf("Expected msg value of 1, got %d", msg.Value())
    }
    // Recv Channel1 (second)
    if msg, ok := <-subClient.Chan(); !ok {
      t.Fatal("subClient chan unexpectedly closed")
    } else if msg.IsClose() {
      t.Fatal("Received Close message")
    } else if name := msg.Channel().Name(); name != ch1.Name() {
      t.Fatalf("Expected msg from channel %s, got from %s", ch1.Name(), name)
    } else if msg.Value() != 1 {
      t.Fatalf("Expected msg value of 1, got %d", msg.Value())
    }
  })

  t.Run("RecvChannel2", func(t *testing.T) {
    // Recv Channel2 (first)
    if msg, err := subClient.RecvTimeout(time.Second); err != nil {
      t.Fatalf("Unexpected error from RecvTimeout: %v", err)
    } else if msg.IsClose() {
      t.Fatal("Received Close message")
    } else if name := msg.Channel().Name(); name != ch2.Name() {
      t.Fatalf("Expected msg from channel %s, got from %s", ch2.Name(), name)
    } else if msg.Value() != 2 {
      t.Fatalf("Expected msg value of 2, got %d", msg.Value())
    }
    // Recv Channel2 (second)
    if msg, err := subClient.RecvTimeout(time.Second); err != nil {
      t.Fatalf("Unexpected error from RecvTimeout: %v", err)
    } else if msg.IsClose() {
      t.Fatal("Received Close message")
    } else if name := msg.Channel().Name(); name != ch2.Name() {
      t.Fatalf("Expected msg from channel %s, got from %s", ch2.Name(), name)
    } else if msg.Value() != 2 {
      t.Fatalf("Expected msg value of 2, got %d", msg.Value())
    }
  })

  // Recv (fail)
  t.Run("Recv (fail)", func(t *testing.T) {
    if msg, err := subClient.RecvTimeout(time.Second); err != ErrTimedOut {
      t.Fatalf("Expected err %v, got %v", ErrTimedOut, err)
    } else if msg.Channel() != nil {
      t.Fatalf("Expected nil channel, got %s", msg.Channel().Name())
    } else if msg.Value() != 0 {
      t.Fatalf("Expected msg value of 0, got %d", msg.Value())
    } else if msg.IsClose() {
      t.Fatal("Expected msg.IsClose() == false, got true")
    }
  })
}

func TestSubAll(t *testing.T) {
  ntwk := NewNetwork[int]()
  pubClient := ntwk.NewClient(0)
  ch1, err := pubClient.NewPub("1", true)
  if err != nil {
    t.Fatalf("Unexpected error: %v", err)
  }

  subClient := ntwk.NewClientSubAll(100)
  defer subClient.Close()

  t.Run("TestPub1", func(t *testing.T) {
    for i := 0; i < 3; i++ {
      ch1.Pub(i)
    }
    for i := 0; i < 3; i++ {
      msg, ok := subClient.TryRecv()
      if !ok {
        t.Fatal("Expected message")
      } else if msg.Value() != i {
        t.Fatalf("Expected %d, got %d", i, msg.Value())
      }
    }
  })

  ch2, err := pubClient.NewPub("2", true)
  if err != nil {
    t.Fatalf("Unexpected error: %v", err)
  }
  t.Run("TestPub1And2", func(t *testing.T) {
    ch2.Pub(2)
    ch1.Pub(1)
    if msg, ok := subClient.TryRecv(); !ok {
      t.Fatal("Expected message")
    } else if msg.Channel().Name() != ch2.Name() {
      t.Fatalf(
        "Expected message from %s, got from %s",
        msg.Channel().Name(), ch2.Name(),
      )
    } else if msg.Value() != 2 {
      t.Fatalf("Expected 2, got %d", msg.Value())
    }
    if msg, ok := subClient.TryRecv(); !ok {
      t.Fatal("Expected message")
    } else if msg.Channel().Name() != ch1.Name() {
      t.Fatalf(
        "Expected message from %s, got from %s",
        msg.Channel().Name(), ch1.Name(),
      )
    } else if msg.Value() != 1 {
      t.Fatalf("Expected 1, got %d", msg.Value())
    }
  })

  t.Run("TestClose2", func(t *testing.T) {
    ch2.Close()
    if msg, ok := subClient.TryRecv(); !ok {
      t.Fatal("Expected message")
    } else if msg.Channel().Name() != ch2.Name() {
      t.Fatalf(
        "Expected message from %s, got from %s",
        msg.Channel().Name(), ch2.Name(),
      )
    } else if !msg.IsClose() {
      t.Fatal("Expected close message")
    }
  })

  t.Run("TestUnsubAll", func(t *testing.T) {
    if !subClient.IsSubbedAll() {
      t.Fatal("Expected client to be subbed all")
    }
    subClient.UnsubAll()
    ch1.Pub(1)
    if msg, ok := subClient.TryRecv(); ok {
      t.Fatalf(
        "Expected no message, got %d from %s",
        msg.Value(), msg.Channel().Name(),
      )
    }
    if subClient.IsSubbedAll() {
      t.Fatal("Expected client not to be subbed all")
    }
  })

  t.Run("TestSubAllAgain", func(t *testing.T) {
    subClient.SubAll()
    ch1.Pub(1)
    if msg, ok := subClient.TryRecv(); !ok {
      t.Fatal("Expected message")
    } else if msg.Channel().Name() != ch1.Name() {
      t.Fatalf(
        "Expected message from %s, got from %s",
        msg.Channel().Name(), ch1.Name(),
      )
    } else if msg.Value() != 1 {
      t.Fatalf("Expected 1, got %d", msg.Value())
    }
    if !subClient.IsSubbedAll() {
      t.Fatal("Expected client to be subbed all")
    }
  })

  ch2, _ = pubClient.NewPub("2", true)
  t.Run("TestPub2", func(t *testing.T) {
    ch2.Pub(2)
    if msg, ok := subClient.TryRecv(); !ok {
      t.Fatal("Expected message")
    } else if msg.Channel().Name() != ch2.Name() {
      t.Fatalf(
        "Expected message from %s, got from %s",
        msg.Channel().Name(), ch2.Name(),
      )
    } else if msg.Value() != 2 {
      t.Fatalf("Expected 2, got %d", msg.Value())
    }
  })

  t.Run("TestClose1", func(t *testing.T) {
    ch1.Close()
    if msg, ok := subClient.TryRecv(); !ok {
      t.Fatal("Expected message")
    } else if msg.Channel().Name() != ch1.Name() {
      t.Fatalf(
        "Expected message from %s, got from %s",
        msg.Channel().Name(), ch1.Name(),
      )
    } else if !msg.IsClose() {
      t.Fatal("Expected close message")
    }
  })

  t.Run("TestUnsubAllAndClose", func(t *testing.T) {
    subClient.UnsubAll()
    pubClient.Close()
    if msg, ok := subClient.TryRecv(); ok {
      t.Fatalf(
        "Expected no message, got %d from %s",
        msg.Value(), msg.Channel().Name(),
      )
    }
  })
}

func TestResize(t *testing.T) {
  const nSend = 100

  // Test Concurrent
  ntwk := NewNetwork[int]()
  pubClient := ntwk.NewClient(0)
  defer pubClient.Close()
  ch, err := pubClient.NewPub("1", false)
  if err != nil {
    t.Fatalf("Unexpected error: %v", err)
  }
  subClient := ntwk.NewClientSubCurrent(nSend * 3)
  defer subClient.Close()

  t.Run("TestResizeConcurrent", func(t *testing.T) {
    start := make(chan struct{})
    done := make(chan struct{}, 2)
    go func() {
      defer func() {
        done <- struct{}{}
      }()
      _, _ = <-start
      for i := 0; i < nSend; i++ {
        ch.Pub(i)
        time.Sleep(time.Millisecond)
      }
    }()

    go func() {
      defer func() {
        done <- struct{}{}
      }()
      _, _ = <-start
      for i, mid := 0, nSend / 2; i < nSend; i++ {
        time.Sleep(time.Millisecond * 5)
        if i == mid {
          subClient.ResizeChan(nSend * 2)
        }
        msg, err := subClient.RecvTimeout(time.Second)
        if err != nil {
          t.Errorf("Unexpected err (%d of %d): %v", i, nSend, err)
          return
        } else if msg.Value() != i {
          t.Errorf("Expected %d, got %d", i, msg.Value())
          return
        }
      }
    }()
    close(start)
    <-done
    <-done
  })
  if t.Failed() {
    t.Fatal("Failed test")
  }

  t.Run("TestResizeSmaller", func(t *testing.T) {
    for i := 0; i < nSend; i++ {
      ch.Pub(i)
    }
    subClient.ResizeChan(nSend / 5)
    for i := 0; i < nSend / 5; i++ {
      msg, ok := subClient.TryRecv()
      if !ok {
        t.Errorf("Unexpected failed recv (%d of %d)", i, nSend)
        return
      } else if msg.Value() != i {
        t.Errorf("Expected %d, got %d", i, msg.Value())
        return
      }
    }
  })
}

func TestSubErrors(t *testing.T) {
  ntwk := NewNetwork[int]()
  pubClient := ntwk.NewClient(0)
  pubClient.NewPub("1", false)

  subClient := ntwk.NewClient(5)
  subClientCurr := ntwk.NewClientSubCurrent(5)
  subClientAll := ntwk.NewClientSubAll(5)

  pubClient.NewPub("2", false)

  // Recv
  t.Run("Recv", func(t *testing.T) {
    if _, err := subClient.RecvTimeout(time.Millisecond); err != ErrTimedOut {
      t.Fatalf("Expected error %v, got %v", ErrTimedOut, err)
    }
    if _, err := subClientCurr.RecvTimeout(time.Millisecond); err != ErrTimedOut {
      t.Fatalf("Expected error %v, got %v", ErrTimedOut, err)
    }
    if _, err := subClientAll.RecvTimeout(time.Millisecond); err != ErrTimedOut {
      t.Fatalf("Expected error %v, got %v", ErrTimedOut, err)
    }
    pubClient.Pub("2", 2)
    if _, err := subClient.RecvTimeout(time.Millisecond); err != ErrTimedOut {
      t.Fatalf("Expected error %v, got %v", ErrTimedOut, err)
    }
    if _, err := subClientCurr.RecvTimeout(time.Millisecond); err != ErrTimedOut {
      t.Fatalf("Expected error %v, got %v", ErrTimedOut, err)
    }
    <-subClientAll.Chan()
  })

  // Sub
  t.Run("Sub", func(t *testing.T) {
    if _, err := subClient.Sub("0"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    }
    if _, err := subClientCurr.Sub("0"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    }
    if _, err := subClientAll.Sub("0"); err != ErrClientSubbedAll {
      t.Fatalf("Expected error %v, got %v", ErrClientSubbedAll, err)
    }
  })

  // SubCurrent
  t.Run("SubCurrent", func(t *testing.T) {
    if err := subClientAll.SubCurrent(); err != ErrClientSubbedAll {
      t.Fatalf("Expected error %v, got %v", ErrClientSubbedAll, err)
    }
  })

  // GetSub
  t.Run("GetSub", func(t *testing.T) {
    if _, err := subClient.GetSub("1"); err != ErrNotSubbed {
      t.Fatalf("Expected error %v, got %v", ErrNotSubbed, err)
    }
    if _, err := subClientCurr.GetSub("2"); err != ErrNotSubbed {
      t.Fatalf("Expected error %v, got %v", ErrNotSubbed, err)
    }
    if _, err := subClientAll.GetSub("1"); err != ErrClientSubbedAll {
      t.Fatalf("Expected error %v, got %v", ErrClientSubbedAll, err)
    }
    if _, err := subClient.GetSub("5"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    }
    if _, err := subClientCurr.GetSub("5"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    }
    if _, err := subClientAll.GetSub("1"); err != ErrClientSubbedAll {
      t.Fatalf("Expected error %v, got %v", ErrClientSubbedAll, err)
    }
  })

  // TODO: GetSubs
  // GetSubs
  subClient.SubCurrent()
  subClientCurr.Sub("2")
  t.Run("Unsub", func(t *testing.T) {
    var errs utils.ErrorSlice
    if _, err := subClient.GetSubs("1", "5", "2"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 1 {
      t.Fatalf("Expected err index 1, got %d", cpe.Index)
    }
    if _, err := subClientCurr.GetSubs("5", "1", "2"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 0 {
      t.Fatalf("Expected err index 0, got %d", cpe.Index)
    }
    if _, err := subClientAll.GetSubs("1", "2", "5"); err != ErrClientSubbedAll {
      t.Fatalf("Expected error %v, got %v", ErrClientSubbedAll, err)
    }
  })

  // GetAllSubs
  t.Run("GetAllSubs", func(t *testing.T) {
    if _, err := subClientAll.GetAllSubs(); err != ErrClientSubbedAll {
      t.Fatalf("Expected error %v, got %v", ErrClientSubbedAll, err)
    }
  })

  // Unsub
  t.Run("Unsub", func(t *testing.T) {
    var errs utils.ErrorSlice
    if err := subClient.Unsub("1", "5", "2"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 1 {
      t.Fatalf("Expected err index 1, got %d", cpe.Index)
    }
    if err := subClientCurr.Unsub("5", "1", "2"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 0 {
      t.Fatalf("Expected err index 0, got %d", cpe.Index)
    }
    if err := subClientAll.Unsub("1", "2", "5"); err != ErrClientSubbedAll {
      t.Fatalf("Expected error %v, got %v", ErrClientSubbedAll, err)
    }
  })

  t.Run("PubClose", func(t *testing.T) {
    if _, err := subClient.Sub("1"); err != nil {
      t.Fatalf("Unexpected error: %v", err)
    }
    pubClient.Close()
    if _, err := subClient.GetSub("1"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    }
  })

  subClientAll.UnsubAll()
  // TODO: subClientAll GetSubs?

  subClient.Close()
  subClientCurr.Close()
  subClientAll.Close()
  t.Run("Closed", func(t *testing.T) {
    if _, err := subClient.GetSub("1"); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if _, err := subClientCurr.GetSub("1"); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if _, err := subClientAll.GetSub("1"); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
  })
}

func TestPubErrors(t *testing.T) {
  ntwk := NewNetwork[int]()
  pubClient1 := ntwk.NewClient(0)
  pubClient1.NewPub("1", false)
  pubClient1.NewPub("2", false)

  pubClient2 := ntwk.NewClient(0)
  pubClient2.NewPub("3", false)
  pubClient2.NewPub("4", false)

  subClient := ntwk.NewClientSubAll(10)

  // GetPub
  t.Run("GetPub", func(t *testing.T) {
    if ch, err := pubClient2.GetPub("1"); err != ErrCantPub {
      t.Fatalf("Expected error %v, got %v", ErrCantPub, err)
    } else if ch != nil {
      t.Fatal("Got non-nil channel")
    }
    if ch, err := pubClient1.GetPub("4"); err != ErrCantPub {
      t.Fatalf("Expected error %v, got %v", ErrCantPub, err)
    } else if ch != nil {
      t.Fatal("Got non-nil channel")
    }
    if ch, err := pubClient2.GetPub("5"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    } else if ch != nil {
      t.Fatal("Got non-nil channel")
    }
  })

  // GetPubs
  t.Run("GetPubs", func(t *testing.T) {
    var errs utils.ErrorSlice
    if _, err := pubClient2.GetPubs("1", "2", "5"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 3 {
      t.Fatalf("Expected 3 errors, got %d", len(errs))
    }
    if _, err := pubClient1.GetPubs("3", "4", "5"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 3 {
      t.Fatalf("Expected 3 errors, got %d", len(errs))
    }

    // GetPubs
    if _, err := pubClient1.GetPubs("1", "5", "2"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 1 {
      t.Fatalf("Expected err index 1, got %d", cpe.Index)
    }
    if _, err := pubClient2.GetPubs("5", "3", "4"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 0 {
      t.Fatalf("Expected err index 0, got %d", cpe.Index)
    }
  })

  // Recv (nothing)
  t.Run("Recv", func(t *testing.T) {
    if msg, ok := subClient.TryRecv(); ok {
      t.Fatalf(
        "Expected no message, received %d from %s (isClosed=%v)",
        msg.Value(), msg.Channel().Name(), msg.IsClose(),
      )
    }
  })

  // Pub
  t.Run("Pub", func(t *testing.T) {
    if ch, err := pubClient2.Pub("2", 2); err != ErrCantPub {
      t.Fatalf("Expected error %v, got %v", ErrCantPub, err)
    } else if ch != nil {
      t.Fatal("Got non-nil channel")
    }
    if ch, err := pubClient1.Pub("3", 3); err != ErrCantPub {
      t.Fatalf("Expected error %v, got %v", ErrCantPub, err)
    } else if ch != nil {
      t.Fatal("Got non-nil channel")
    }
    if ch, err := pubClient1.Pub("5", 5); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    } else if ch != nil {
      t.Fatal("Got non-nil channel")
    }
  })

  // Recv (nothing)
  t.Run("RecvAgain", func(t *testing.T) {
    if msg, ok := subClient.TryRecv(); ok {
      t.Fatalf(
        "Expected no message, received %d from %s (isClosed=%v)",
        msg.Value(), msg.Channel().Name(), msg.IsClose(),
      )
    }
  })

  // Close
  t.Run("ClosePub", func(t *testing.T) {
    if err := pubClient2.ClosePub("1"); err != ErrCantPub {
      t.Fatalf("Expected error %v, got %v", ErrCantPub, err)
    }
    if err := pubClient1.ClosePub("4"); err != ErrCantPub {
      t.Fatalf("Expected error %v, got %v", ErrCantPub, err)
    }
    if err := pubClient2.ClosePub("5"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    }
  })

  // ClosePubs
  t.Run("ClosePubs", func(t *testing.T) {
    var errs utils.ErrorSlice
    if err := pubClient2.ClosePubs("1", "2", "5"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 3 {
      t.Fatalf("Expected 3 errors, got %d", len(errs))
    }
    if err := pubClient1.ClosePubs("3", "4", "5"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 3 {
      t.Fatalf("Expected 3 errors, got %d", len(errs))
    }

    // ClosePubs
    if err := pubClient1.ClosePubs("1", "5", "2"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 1 {
      t.Fatalf("Expected err index 1, got %d", cpe.Index)
    }
    if err := pubClient2.ClosePubs("5", "3", "4"); !errors.As(err, &errs) {
      t.Fatalf("Expected utils.ErrorsSlice, got (%T) %v", err, err)
    } else if len(errs) != 1 {
      t.Fatalf("Expected 1 error, got %d", len(errs))
    } else if cpe, ok := errs[0].(utils.ElemError); !ok {
      t.Fatalf("Expected utils.ElemError, got %T", errs[0])
    } else if cpe.Index != 0 {
      t.Fatalf("Expected err index 0, got %d", cpe.Index)
    }
  })

  // Create
  ch1, _ := pubClient1.NewPub("1", false)
  ch2, _ := pubClient1.NewPub("2", false)

  // Close (successful)
  if err := ch1.Close(); err != nil {
    t.Fatalf("Unexpected error: %v", err)
  }
  pubClient1.closeAllPubs()

  // Close
  t.Run("CloseChan", func(t *testing.T) {
    if err := pubClient1.ClosePub("1"); err != ErrChanNotExist {
      t.Fatalf("Expected error %v, got %v", ErrChanNotExist, err)
    }
    if err := ch1.Close(); err != ErrChanClosed {
      t.Fatalf("Expected error %v, got %v", ErrChanClosed, err)
    }
  })

  // Pub
  t.Run("PubAgain", func(t *testing.T) {
    if err := ch1.Pub(1); err != ErrChanClosed {
      t.Fatalf("Expected error %v, got %v", ErrChanClosed, err)
    }
    if err := ch2.Pub(1); err != ErrChanClosed {
      t.Fatalf("Expected error %v, got %v", ErrChanClosed, err)
    }
  })

  // Close client
  pubClient1.Close()
  ch3, _ := pubClient2.NewPub("3", false)
  pubClient2.Close()

  // Test all
  t.Run("TestAll", func(t *testing.T) {
    // TODO: ClosePubs
    if _, err := pubClient1.NewPub("1", false); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if _, err := pubClient1.Pub("1", 1); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if err := pubClient1.ClosePub("1"); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if err := pubClient1.ClosePubs("1", "2"); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if err := pubClient1.CloseAllPubs(); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if _, err := pubClient2.GetAllPubs(); err != ErrClientClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if err := ch3.Pub(1); err != ErrChanClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
    if err := ch3.Close(); err != ErrChanClosed {
      t.Fatalf("Expected error %v, got %v", ErrClientClosed, err)
    }
  })
}

func TestClientSubAll(t *testing.T) {
  const (
    nPubs, nChansEach, nSendEach = 10, 10, 10
    msgsRecvEach = nPubs * nChansEach * nSendEach
    nSubs, chanLen = 3, 5
  )
  // TODO
  var subsWg sync.WaitGroup
  for i := 0; i < 5; i++ {
    //
    subsWg.Add(1)
    go func(id int) {
      defer subsWg.Done()
    }(i+1)
  }
}

func TestNewClientSubCurrent(t *testing.T) {
  const (
    nPubs, nChansEach, nSendEach = 5, 5, 5
    totalMsgs = nPubs * nChansEach * nSendEach
  )
  ntwk := NewNetwork[int]()
  pubClients := make([]*Client[int], 0, nPubs)
  for i := 1; i <= nPubs; i++ {
    pub := ntwk.NewClient(0)
    defer pub.Close()
    pubClients = append(pubClients, pub)
    for j := 1; j <= nChansEach; j++ {
      chName := fmt.Sprintf("%d-%d", i, j)
      if ch, err := pub.NewPub(chName, false); err != nil {
        t.Fatalf("Error creating pub %s: %v", chName, err)
      } else if ch == nil {
        t.Fatalf("NewPub returned nil channel for %s", chName)
      }
    }
  }

  subClient := ntwk.NewClientSubCurrent(totalMsgs)
  defer subClient.Close()

  for i := 0; i < nPubs; i++ {
    pub := pubClients[i]
    chans, err := pub.GetAllPubs()
    if err != nil {
      t.Fatalf("Error getting all pubs for %d: %v", i+1, err)
    } else if l := len(chans); l != nChansEach {
      t.Fatalf("Expected %d chans, got %d", nChansEach, l)
    }
    for j, ch := range chans {
      chName := fmt.Sprintf("%d-%d", i+1, j+1)
      for k := 0; k < nSendEach; k++ {
        val := (i * nPubs*nPubs) + (j * nChansEach) + k
        if j % 2 == 0 {
          if err := ch.Pub(val); err != nil {
            t.Fatalf("Unexpected error publishing to %s: %v", chName, err)
          }
        } else {
          if _, err := pub.Pub(chName, val); err != nil {
            t.Fatalf("Unexpected error publishing to %s: %v", chName, err)
          }
        }
      }
    }
  }

  ints := make(map[int]bool, totalMsgs)
  for i := 0; i < totalMsgs; i++ {
    select {
    case msg, ok := <-subClient.Chan():
      if !ok {
        t.Fatal("Channel unexpectedly closed")
      } else if msg.IsClose() {
        t.Fatal("Unexpected close messasge on channel ", msg.Channel().Name())
      } else {
        ints[msg.Value()] = true
      }
    default:
      t.Fatalf("Expected %d messages, got %d", totalMsgs, i)
    }
  }
  if l := len(ints); l != totalMsgs {
    t.Fatalf("Expected %d unique messages, got %d", totalMsgs, l)
  }
}

func TestSubCurrentConcurrent(t *testing.T) {
  const (
    nPubs, nSendEach = 30, 10
    // This is set to 330 to handle the 30 close messages. Occassionally, the
    // subs don't read fast enough and some messages are dropped
    nSubs, nChanLen = 30, 330
    totalRecvEach = nSendEach * nPubs
  )
  var wg sync.WaitGroup
  start := make(chan bool)
  ntwk := NewNetwork[int]()
  var nSent atomic.Uint32
  // Make pubs
  for i := 0; i < nPubs; i++ {
    id := i + 1
    wg.Add(1)
    go func(id int) {
      client := ntwk.NewClient(0)
      chName := fmt.Sprint(id)
      ch, err := client.NewPub(chName, true)
      if err != nil {
        t.Errorf("Could not create pub channel for id %d: %v", id, err)
        wg.Done()
        return
      }
      defer client.Close()

      if id % 3 == 0 {
        ch, err = client.GetPub(chName)
        if err != nil {
          t.Errorf("Could not get pub channel for id %d: %v", id, err)
          wg.Done()
          return
        }
      }
      wg.Done()

      _, _ = <-start
      for i := 0; i < nSendEach; i++ {
        if id % 3 == 1 {
          if ch, err = client.Pub(chName, i); err != nil {
            t.Errorf("Pub %d failed to pub from client: %v", id, err)
            return
          }
        } else {
          if err = ch.Pub(i); err != nil {
            t.Errorf("Pub %d failed to pub to channel: %v", id, err)
            return
          }
        }
        nSent.Add(1)
      }
    }(id)
  }
  wg.Wait()
  if t.Failed() {
    t.Fatal("Failed preconditions")
  }

  // Make subs
  timer := newTimer(time.Second * 5)
  var nRecvd atomic.Uint32
  var subsWg sync.WaitGroup
  for i := 0; i < nSubs; i++ {
    wg.Add(1)
    subsWg.Add(1)
    go func(id int) {
      defer subsWg.Done()
      var client *Client[int]
      switch id % 4 {
      case 0:
        client = ntwk.NewClientSubAll(nChanLen)
      case 1:
        client = ntwk.NewClientSubCurrent(nChanLen)
      case 2:
        client = ntwk.NewClient(nChanLen)
        client.SubAll()
      default:
        client = ntwk.NewClient(nChanLen)
        client.SubCurrent()
      }
      wg.Done()

      _, _ = <-start

      done, ch := false, client.Chan()
      nGot, nClosed := 0, 0
      for !done {
        select {
        case msg, ok := <-ch:
          if msg.IsClose() {
            nClosed++
            done = nClosed == nPubs
          } else if !ok {
            t.Errorf("Sub %d unexpectedly closed", id)
          } else {
            nGot++
            nRecvd.Add(1)
          }
        case _, _ = <-timer.c:
          t.Errorf(
            "Sub %d exceeded time limit, got %d of %d",
            id, nGot, totalRecvEach,
          )
          done = true
        }
      }
    }(i+1)
  }
  wg.Wait()
  close(start)
  timer.Start()

  subsWg.Wait()
  ns, nr := nSent.Load(), nRecvd.Load()
  expectedTotalRecvd := ns * nSubs
  if nr != expectedTotalRecvd {
    t.Fatalf("Expected %d, got %d", expectedTotalRecvd, nr)
  }
}

type Timer struct {
  started atomic.Bool
  c chan struct{}
  dur time.Duration
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
    c: make(chan struct{}),
    dur: dur,
  }
}
