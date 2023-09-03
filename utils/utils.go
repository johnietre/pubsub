package utils

import (
  "encoding/binary"
	"errors"
	"sync"
)

// MsgType represents a message type.
type MsgType byte

const (
  // MsgTypeErr represents an error message.
	MsgTypeErr MsgType = iota
  // MsgTypeSub represents a subscription message.
	MsgTypeSub
	// MsgTypeUnsub represents an unsubscribe message.
	MsgTypeUnsub
	// MsgTypePub represents a publish message.
	MsgTypePub
	// MsgTypeNewChan represents a channel creation message.
	MsgTypeNewChan
	// MsgTypeNewMultiChan represents a multi-channel creation message.
	MsgTypeNewMultiChan
	// MsgTypeDelChan represents a delete channel message.
	MsgTypeDelChan
	// MsgTypeChanNames represents a message requesting channel names.
	MsgTypeChanNames
	// MsgTypeOk represents a successful (ok) action (generally a response).
	MsgTypeOk
)

var (
  // ErrBadMsg represents a bad message.
	ErrBadMsg      = errors.New("malformed message")
  // ErrMismatchLen is returned when the provided length match the actual.
	ErrMismatchLen = errors.New("mismatch content length")
)

// EncodeMsg encodes a message into bytes.
func EncodeMsg(mt MsgType, contents string) []byte {
	return append([]byte{byte(mt)}, append(Put4(uint32(len(contents))), contents...)...)
}

// DecodeMsg decodes a message and its type from bytes.
func DecodeMsg(msg []byte) (MsgType, string, error) {
	l := len(msg)
	if l < 5 {
		return MsgTypeErr, "", ErrBadMsg
	}
	mt := MsgType(msg[0])
	cl := int(Get4(msg[1:5]))
	if cl+5 != l {
		return MsgTypeErr, "", ErrMismatchLen
	}
	return mt, string(msg[5 : 5+cl]), nil
}

// Put4 encodes a uint32 as big-endian bytes.
func Put4(u uint32) []byte {
  return binary.BigEndian.AppendUint32(nil, u)
  /*
	return []byte{
		byte(u << 24),
		byte(u << 16),
		byte(u << 8),
		byte(u),
	}
  */
}

// Get4 decodes a uint32 from big-endian bytes.
func Get4(b []byte) uint32 {
  return binary.BigEndian.Uint32(b)
	//return uint32(b[0])>>24 | uint32(b[1])>>16 | uint32(b[2])>>8 | uint32(b[3])
}

// SyncMap is a wrapper for sync.Map using generics.
type SyncMap[K any, V any] struct {
	m sync.Map
}

// NewSyncMap creates a new SyncMap
func NewSyncMap[K any, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{}
}

// Load loads a value for the given key if it exists.
func (sm *SyncMap[K, V]) Load(key K) (v V, loaded bool) {
	var val any
	val, loaded = sm.m.Load(key)
	if loaded {
		v = val.(V)
	}
	return
}

// Store stores the given key-value pair.
func (sm *SyncMap[K, V]) Store(key K, val V) {
  sm.m.Store(key, val)
}

// LoadOrStore loads the value for the given key if it exists, else stores it.
func (sm *SyncMap[K, V]) LoadOrStore(key K, val V) (v V, loaded bool) {
	var value any
	value, loaded = sm.m.LoadOrStore(key, val)
	return value.(V), loaded
}

// LoadAndDelete loads the value for the given key and deletes it if it exists.
func (sm *SyncMap[K, V]) LoadAndDelete(key K) (v V, loaded bool) {
	var val any
	val, loaded = sm.m.LoadAndDelete(key)
	if loaded {
		v = val.(V)
	}
	return
}

// Delete deletes the key-value pair for the given key.
func (sm *SyncMap[K, V]) Delete(key K) {
	sm.m.Delete(key)
}

// Range iterates over the key-value pairs, passing them to the passed
// function. If the passed function returns false, iteration stops.
func (sm *SyncMap[K, V]) Range(f func(K, V) bool) {
	sm.m.Range(func(key, val any) bool {
		return f(key.(K), val.(V))
	})
}

// Set represents a hash set.
type Set[T comparable] struct {
	m map[T]Unit
}

// NewSet creates a new Set.
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{m: make(map[T]Unit)}
}

// Insert inserts a value, returning true if the value didn't exist.
func (s *Set[T]) Insert(item T) bool {
	if _, ok := s.m[item]; !ok {
		s.m[item] = Unit{}
		return true
	}
	return false
}

// Remove deletes a value, returning true if the value existed.
func (s *Set[T]) Remove(item T) bool {
	if _, ok := s.m[item]; !ok {
		delete(s.m, item)
		return true
	}
	return false
}

// Contains returns whether the set contains the item.
func (s *Set[T]) Contains(item T) bool {
	_, ok := s.m[item]
	return ok
}

// Range iterates over each item in random order, applying a given function
// that returns whether the iterations should stop.
func (s *Set[T]) Range(f func(T) bool) {
	for item := range s.m {
		if !f(item) {
			return
		}
	}
}

// SyncSet represents a hash set using sync.Map and generics.
type SyncSet[T any] struct {
	m sync.Map
}

// NewSyncSet creates a new SyncSet
func NewSyncSet[T any]() *SyncSet[T] {
	return &SyncSet[T]{}
}

// Insert inserts a value, returning true if the value didn't exist
func (s *SyncSet[T]) Insert(item T) bool {
	_, loaded := s.m.LoadOrStore(item, Unit{})
	return !loaded
}

// Remove deletes a value, returning true if the value existed
func (s *SyncSet[T]) Remove(item T) bool {
	_, loaded := s.m.LoadAndDelete(item)
	return loaded
}

// Contains returns whether the set contains the item
func (s *SyncSet[T]) Contains(item T) bool {
	_, loaded := s.m.Load(item)
	return loaded
}

// Range iterates over each item in random order, applying a given function
// that returns whether the iterations should stop
func (s *SyncSet[T]) Range(f func(T) bool) {
	s.m.Range(func(k, _ any) bool {
		return f(k.(T))
	})
}

// Unit is an empty struct (struct{}).
type Unit struct{}

// Select1 selects from a single channel c in a loop and outputs to channel out
// until a value is sent on the returned channel, or that or c is closed.
func Select1[T any](c <-chan T, out chan<- T) chan Unit {
  stopChan := make(chan Unit, 1)
  go func() {
    select1(c, out, stopChan)
    close(out)
  }()
  return stopChan
}

func select1[T any](c <-chan T, out chan<- T, stopChan <-chan Unit) {
  for {
    select {
    case t, ok := <-c:
      if !ok {
        return
      }
      out <- t
    case _, _ = <-stopChan:
      return
    }
  }
}

// Select2 is the same as Select1 but selects from c1 and c2. If c1 or c2 is
// closed, the remaining channel is passed to Select1 to continue on.
func Select2[T any](c1, c2 <-chan T, out chan<- T) chan Unit {
  stopChan := make(chan Unit, 1)
  go func() {
    select2(c1, c2, out, stopChan)
    close(out)
  }()
  return stopChan
}

func select2[T any](c1, c2 <-chan T, out chan<- T, stopChan <-chan Unit) {
  var ok bool
  var t T
  for {
    select {
    case t, ok = <-c1:
      if !ok {
        select1(c2, out, stopChan)
        return
      }
    case t, ok = <-c2:
      if !ok {
        select1(c1, out, stopChan)
        return
      }
    case _, _ = <-stopChan:
      return
    }
    out <- t
  }
}

// Select3 is the same as Select2 but with 3 channels.
func Select3[T any](c1, c2, c3 <-chan T, out chan<- T) chan Unit {
  stopChan := make(chan Unit, 1)
  go func() {
    select3(c1, c2, c3, out, stopChan)
    close(out)
  }()
  return stopChan
}

func select3[T any](c1, c2, c3 <-chan T, out chan<- T, stopChan chan Unit) {
  var ok bool
  var t T
  for {
    select {
    case t, ok = <-c1:
      if !ok {
        select2(c2, c3, out, stopChan)
        return
      }
    case t, ok = <-c2:
      if !ok {
        select2(c1, c3, out, stopChan)
        return
      }
    case t, ok = <-c3:
      if !ok {
        select2(c1, c2, out, stopChan)
        return
      }
    case _, _ = <-stopChan:
      return
    }
    out <- t
  }
}

// SelectN selects over n channels and sends all output to out. Uses
// Select[1,2,3] under the hood.
func SelectN[T any](out chan<- T, chans ...<-chan T) chan Unit {
  stopChan, stopChans := make(chan Unit, 1), make([]chan Unit, 0)
  var wg sync.WaitGroup
  for l := len(chans); l > 0; l = len(chans) {
    if l >= 3 {
      stop, c1, c2, c3 := make(chan Unit), chans[0], chans[1], chans[2]
      stopChans = append(stopChans, stop)
      wg.Add(1)
      go func() {
        select3(c1, c2, c3, out, stop)
        wg.Done()
      }()
      chans = chans[3:]
    } else if l >= 2 {
      stop, c1, c2 := make(chan Unit), chans[0], chans[1]
      stopChans = append(stopChans, stop)
      wg.Add(1)
      go func() {
        select2(c1, c2, out, stop)
        wg.Done()
      }()
      chans = chans[2:]
    } else {
      stop, c := make(chan Unit), chans[0]
      stopChans = append(stopChans, stop)
      wg.Add(1)
      go func() {
        select1(c, out, stop)
        wg.Done()
      }()
      chans = chans[1:]
    }
  }
  internalStopChan := make(chan Unit, 0)
  go func() {
    wg.Wait()
    close(out)
    close(internalStopChan)
  }()
  go func() {
    select {
    case _, _ = <-stopChan:
      // This is only run in this case since if internalStopChan is closed, it
      // means all the workers are done so there's no need to close the
      // channels
      for _, c := range stopChans {
        close(c)
      }
    case _, _ = <-internalStopChan:
    }
  }()
  return stopChan
}
