package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
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

// ElemError is useful for reporting errors with specific indexes of slices.
// Especially useful when used in tandem with ErrorSlice.
type ElemError struct {
	Index int
	Err   error
}

func NewElemError(index int, err error) ElemError {
	return ElemError{Index: index, Err: err}
}

// Error implements the error interface.
func (ee ElemError) Error() string {
	return fmt.Sprintf("Index %d: %v", ee.Index, ee.Err)
}

// Is implements the errors.Is interface.
func (ee ElemError) Is(target error) bool {
	return errors.Is(ee.Err, target)
}

// As implements the errors.As interface.
func (ee ElemError) As(target any) bool {
	return errors.As(ee.Err, target)
}

// Unwrap implements the errors.Unwrap interface.
func (ee ElemError) Unwrap() error {
	return ee.Err
}

// ErrorSlice is a slice of multiple errors
type ErrorSlice []error

// NewErrorsSlice returns a new slice with the nil values removed.
func NewErrorSlice(errs ...error) ErrorSlice {
	for i := range errs {
		if errs[i] == nil {
			errs = append(errs[:i], errs[i+1:]...)
		}
	}
	return ErrorSlice(errs)
}

// NewErrorsSliceKeep returns a new slice keeping the nil values.
func NewErrorSliceKeep(errs ...error) ErrorSlice {
	return ErrorSlice(errs)
}

// Error implements the error interface. Doesn't include nil values in the
// resulting string.
func (es ErrorSlice) Error() string {
	errStr := ""
	for _, e := range es {
		if e != nil {
			errStr += e.Error() + "\n"
		}
	}
	if errStr != "" {
		errStr = errStr[:len(errStr)-1]
	}
	return errStr
}

var (
	// ErrBadMsg represents a bad message.
	ErrBadMsg = errors.New("malformed message")
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
}

// Get4 decodes a uint32 from big-endian bytes.
func Get4(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
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
type Unit = struct{}

// NOTE: The rest of the code contains a number of Select* functions for
// selecting over a number of channels. Technically, the reflect.Select
// function could be used to select over an arbitrary number of channels, but,
// to the best of my knowledge, this is not optimal (see https://www.reddit.com/r/golang/comments/7iw9s6/selecting_from_an_array_of_channels/).
// These functions also eliminate the need for spawning a goroutine for each
// channel.
// Thus, these are here for convience and SelectN is used as a replacement for
// reflect.Select.

// Select1 selects from a single channel c in a loop and outputs to channel out
// until a value is sent on the returned channel, or that or c is closed. If
// the "stop chan" (what's returned) is used to stop the function, the function
// will attempt to drain what's left of channel c into the out channel (will
// not block on a channel send or receive). This is done by taking the length l
// of c after the returned channel is used and attempting read l messages from
// c. This functionality is in all Select* functions, and is spread across
// their respective number of channels (e.g., for Select4, this is done for
// each of the 4 passed channels). Afterwards, the `out` chan passed to the
// function is closed.
func Select1[T any](c <-chan T, out chan<- T) chan<- Unit {
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
			// Attempt to drain the rest of the channel's messages into `out`
			l, ok := len(c), true
			for i := 0; i < l && ok; i++ {
				select {
				case t, ok := <-c:
					if ok {
						select {
						case out <- t:
						default:
						}
					}
				default:
				}
			}
			return
		}
	}
}

// Select2 is the same as Select1 but selects from c1 and c2. If c1 or c2 is
// closed, the remaining channel is passed to Select1 to continue on.
func Select2[T any](c1, c2 <-chan T, out chan<- T) chan<- Unit {
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
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range []<-chan T{c1, c2} {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select3 is the same as Select2 but with 3 channels.
func Select3[T any](c1, c2, c3 <-chan T, out chan<- T) chan<- Unit {
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
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range []<-chan T{c1, c2, c3} {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select4 is the same as Select2 but with 4 channels.
func Select4[T any](c1, c2, c3, c4 <-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select4(c1, c2, c3, c4, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select4[T any](
	c1, c2, c3, c4 <-chan T, out chan<- T, stopChan chan Unit,
) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-c1:
			if !ok {
				select3(c2, c3, c4, out, stopChan)
				return
			}
		case t, ok = <-c2:
			if !ok {
				select3(c1, c3, c4, out, stopChan)
				return
			}
		case t, ok = <-c3:
			if !ok {
				select3(c1, c2, c4, out, stopChan)
				return
			}
		case t, ok = <-c4:
			if !ok {
				select3(c1, c2, c3, out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range []<-chan T{c1, c2, c3, c4} {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select5 is the same as Select2 but with 5 channels.
func Select5[T any](c1, c2, c3, c4, c5 <-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select5(c1, c2, c3, c4, c5, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select5[T any](
	c1, c2, c3, c4, c5 <-chan T, out chan<- T, stopChan chan Unit,
) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-c1:
			if !ok {
				select4(c2, c3, c4, c5, out, stopChan)
				return
			}
		case t, ok = <-c2:
			if !ok {
				select4(c1, c3, c4, c5, out, stopChan)
				return
			}
		case t, ok = <-c3:
			if !ok {
				select4(c1, c2, c4, c5, out, stopChan)
				return
			}
		case t, ok = <-c4:
			if !ok {
				select4(c1, c2, c3, c5, out, stopChan)
				return
			}
		case t, ok = <-c5:
			if !ok {
				select4(c1, c2, c3, c4, out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range []<-chan T{c1, c2, c3, c4, c5} {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select6 is the same as Select2 but with 6 channels.
func Select6[T any](
	c1, c2, c3, c4, c5, c6 <-chan T, out chan<- T,
) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select6(c1, c2, c3, c4, c5, c6, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select6[T any](
	c1, c2, c3, c4, c5, c6 <-chan T, out chan<- T, stopChan chan Unit,
) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-c1:
			if !ok {
				select5(c2, c3, c4, c5, c6, out, stopChan)
				return
			}
		case t, ok = <-c2:
			if !ok {
				select5(c1, c3, c4, c5, c6, out, stopChan)
				return
			}
		case t, ok = <-c3:
			if !ok {
				select5(c1, c2, c4, c5, c6, out, stopChan)
				return
			}
		case t, ok = <-c4:
			if !ok {
				select5(c1, c2, c3, c5, c6, out, stopChan)
				return
			}
		case t, ok = <-c5:
			if !ok {
				select5(c1, c2, c3, c4, c6, out, stopChan)
				return
			}
		case t, ok = <-c6:
			if !ok {
				select5(c1, c2, c3, c4, c5, out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range []<-chan T{c1, c2, c3, c4, c5, c6} {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select7 is the same as Select2 but with 7 channels.
func Select7[T any](cs [7]<-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select7(cs, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select7[T any](cs [7]<-chan T, out chan<- T, stopChan chan Unit) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-cs[0]:
			if !ok {
				select6(cs[1], cs[2], cs[3], cs[4], cs[5], cs[6], out, stopChan)
				return
			}
		case t, ok = <-cs[1]:
			if !ok {
				select6(cs[0], cs[2], cs[3], cs[4], cs[5], cs[6], out, stopChan)
				return
			}
		case t, ok = <-cs[2]:
			if !ok {
				select6(cs[0], cs[1], cs[3], cs[4], cs[5], cs[6], out, stopChan)
				return
			}
		case t, ok = <-cs[3]:
			if !ok {
				select6(cs[0], cs[1], cs[2], cs[4], cs[5], cs[6], out, stopChan)
				return
			}
		case t, ok = <-cs[4]:
			if !ok {
				select6(cs[0], cs[1], cs[2], cs[3], cs[5], cs[6], out, stopChan)
				return
			}
		case t, ok = <-cs[5]:
			if !ok {
				select6(cs[0], cs[1], cs[2], cs[3], cs[4], cs[6], out, stopChan)
				return
			}
		case t, ok = <-cs[6]:
			if !ok {
				select6(cs[0], cs[1], cs[2], cs[3], cs[4], cs[5], out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range cs {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select8 is the same as Select2 but with 8 channels.
func Select8[T any](cs [8]<-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select8(cs, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select8[T any](cs [8]<-chan T, out chan<- T, stopChan chan Unit) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-cs[0]:
			if !ok {
				// Leaves out the first element
				select7(*(*[7]<-chan T)(cs[1:]), out, stopChan)
				return
			}
		case t, ok = <-cs[1]:
			if !ok {
				select7(
					[7]<-chan T{
						cs[0], cs[2], cs[3], cs[4], cs[5], cs[6], cs[7],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[2]:
			if !ok {
				select7(
					[7]<-chan T{
						cs[0], cs[1], cs[3], cs[4], cs[5], cs[6], cs[7],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[3]:
			if !ok {
				select7(
					[7]<-chan T{
						cs[0], cs[1], cs[2], cs[4], cs[5], cs[6], cs[7],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[4]:
			if !ok {
				select7(
					[7]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[5], cs[6], cs[7],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[5]:
			if !ok {
				select7(
					[7]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[6], cs[7],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[6]:
			if !ok {
				select7(
					[7]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[5], cs[7],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[7]:
			if !ok {
				// Leaves out the last element
				select7(*(*[7]<-chan T)(cs[:]), out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range cs {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select9 is the same as Select2 but with 9 channels.
func Select9[T any](cs [9]<-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select9(cs, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select9[T any](cs [9]<-chan T, out chan<- T, stopChan chan Unit) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-cs[0]:
			if !ok {
				// Leaves out the first element
				select8(*(*[8]<-chan T)(cs[1:]), out, stopChan)
				return
			}
		case t, ok = <-cs[1]:
			if !ok {
				select8(
					[8]<-chan T{
						cs[0], cs[2], cs[3], cs[4], cs[5], cs[6], cs[7], cs[8],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[2]:
			if !ok {
				select8(
					[8]<-chan T{
						cs[0], cs[1], cs[3], cs[4], cs[5], cs[6], cs[7], cs[8],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[3]:
			if !ok {
				select8(
					[8]<-chan T{
						cs[0], cs[1], cs[2], cs[4], cs[5], cs[6], cs[7], cs[8],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[4]:
			if !ok {
				select8(
					[8]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[5], cs[6], cs[7], cs[8],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[5]:
			if !ok {
				select8(
					[8]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[6], cs[7], cs[8],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[6]:
			if !ok {
				select8(
					[8]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[5], cs[7], cs[8],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[7]:
			if !ok {
				select8(
					[8]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[5], cs[6], cs[8],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[8]:
			if !ok {
				// Leaves out the last element
				select8(*(*[8]<-chan T)(cs[:]), out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range cs {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select10 is the same as Select2 but with 10 channels.
func Select10[T any](cs [10]<-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select10(cs, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select10[T any](cs [10]<-chan T, out chan<- T, stopChan chan Unit) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-cs[0]:
			if !ok {
				// Leaves out the first element
				select9(*(*[9]<-chan T)(cs[1:]), out, stopChan)
				return
			}
		case t, ok = <-cs[1]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[2], cs[3], cs[4], cs[5], cs[6], cs[7], cs[8], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[2]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[1], cs[3], cs[4], cs[5], cs[6], cs[7], cs[8], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[3]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[1], cs[2], cs[4], cs[5], cs[6], cs[7], cs[8], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[4]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[5], cs[6], cs[7], cs[8], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[5]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[6], cs[7], cs[8], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[6]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[5], cs[7], cs[8], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[7]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[5], cs[6], cs[8], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[8]:
			if !ok {
				select9(
					[9]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4], cs[5], cs[6], cs[7], cs[9],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[9]:
			if !ok {
				// Leaves out the last element
				select9(*(*[9]<-chan T)(cs[:]), out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range cs {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select11 is the same as Select2 but with 11 channels.
func Select11[T any](cs [11]<-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select11(cs, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select11[T any](cs [11]<-chan T, out chan<- T, stopChan chan Unit) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-cs[0]:
			if !ok {
				// Leaves out the first element
				select10(*(*[10]<-chan T)(cs[1:]), out, stopChan)
				return
			}
		case t, ok = <-cs[1]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[2], cs[3], cs[4], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[2]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[3], cs[4], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[3]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[2], cs[4], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[4]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[5]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[6], cs[7], cs[8], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[6]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[7], cs[8], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[7]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[6], cs[8], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[8]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[6], cs[7], cs[9], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[9]:
			if !ok {
				select10(
					[10]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[6], cs[7], cs[8], cs[10],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[10]:
			if !ok {
				// Leaves out the last element
				select10(*(*[10]<-chan T)(cs[:]), out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range cs {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// Select12 is the same as Select2 but with 12 channels.
func Select12[T any](cs [12]<-chan T, out chan<- T) chan<- Unit {
	stopChan := make(chan Unit, 1)
	go func() {
		select12(cs, out, stopChan)
		close(out)
	}()
	return stopChan
}

func select12[T any](cs [12]<-chan T, out chan<- T, stopChan chan Unit) {
	var ok bool
	var t T
	for {
		select {
		case t, ok = <-cs[0]:
			if !ok {
				// Leaves out the first element
				select11(*(*[11]<-chan T)(cs[1:]), out, stopChan)
				return
			}
		case t, ok = <-cs[1]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[2], cs[3], cs[4], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[2]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[3], cs[4], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[3]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[4], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[4]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[5],
						cs[6], cs[7], cs[8], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[5]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[6], cs[7], cs[8], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[6]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[7], cs[8], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[7]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[6], cs[8], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[8]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[6], cs[7], cs[9], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[9]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[6], cs[7], cs[8], cs[10], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[10]:
			if !ok {
				select11(
					[11]<-chan T{
						cs[0], cs[1], cs[2], cs[3], cs[4],
						cs[5], cs[6], cs[7], cs[8], cs[9], cs[11],
					},
					out, stopChan,
				)
				return
			}
		case t, ok = <-cs[11]:
			if !ok {
				// Leaves out the last element
				select11(*(*[11]<-chan T)(cs[:]), out, stopChan)
				return
			}
		case _, _ = <-stopChan:
			// Attempt to drain the rest of the channels' messages into `out`
			for _, ch := range cs {
				l, ok := len(ch), true
				for i := 0; i < l && ok; i++ {
					select {
					case t, ok = <-ch:
						if ok {
							select {
							case out <- t:
							default:
							}
						}
					default:
					}
				}
			}
			return
		}
		out <- t
	}
}

// SelectN selects over n channels and sends all output to out. Uses
// Select[1,2,3,...] under the hood. If the number of channels to select over
// is already known, it is more effecient to call one of the other select
// methods like Select2 or Select3 since the number of goroutines spawned is
// always either (N - 1) / S + 1 + 2 (where N = the number of chans passed and
// S is the highest number of channels that can be passed to a given Select*
// function [currently 12, i.e., Select12]).
func SelectN[T any](out chan<- T, chans ...<-chan T) chan<- Unit {
	stopChan, stopChans := make(chan Unit, 1), make([]chan Unit, 0)
	var wg sync.WaitGroup
	for l := len(chans); l > 0; l = len(chans) {
		switch l % 12 {
		case 0:
			stop, cs := make(chan Unit), chans
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select12(*(*[12]<-chan T)(cs), out, stop)
				wg.Done()
			}()
			chans = chans[12:]
		case 11:
			stop, cs := make(chan Unit), chans
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select11(*(*[11]<-chan T)(cs), out, stop)
				wg.Done()
			}()
			chans = chans[11:]
		case 10:
			stop, cs := make(chan Unit), chans
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select10(*(*[10]<-chan T)(cs), out, stop)
				wg.Done()
			}()
			chans = chans[10:]
		case 9:
			stop, cs := make(chan Unit), chans
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select9(*(*[9]<-chan T)(cs), out, stop)
				wg.Done()
			}()
			chans = chans[9:]
		case 8:
			stop, cs := make(chan Unit), chans
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select8(*(*[8]<-chan T)(cs), out, stop)
				wg.Done()
			}()
			chans = chans[8:]
		case 7:
			stop, cs := make(chan Unit), chans
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select7(*(*[7]<-chan T)(cs), out, stop)
				wg.Done()
			}()
			chans = chans[7:]
		case 6:
			stop := make(chan Unit)
			c1, c2, c3, c4, c5 := chans[0], chans[1], chans[2], chans[3], chans[4]
			c6 := chans[5]
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select6(c1, c2, c3, c4, c5, c6, out, stop)
				wg.Done()
			}()
			chans = chans[6:]
		case 5:
			stop := make(chan Unit)
			c1, c2, c3, c4, c5 := chans[0], chans[1], chans[2], chans[3], chans[4]
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select5(c1, c2, c3, c4, c5, out, stop)
				wg.Done()
			}()
			chans = chans[5:]
		case 4:
			stop := make(chan Unit)
			c1, c2, c3, c4 := chans[0], chans[1], chans[2], chans[3]
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select4(c1, c2, c3, c4, out, stop)
				wg.Done()
			}()
			chans = chans[4:]
		case 3:
			stop, c1, c2, c3 := make(chan Unit), chans[0], chans[1], chans[2]
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select3(c1, c2, c3, out, stop)
				wg.Done()
			}()
			chans = chans[3:]
		case 2:
			stop, c1, c2 := make(chan Unit), chans[0], chans[1]
			stopChans = append(stopChans, stop)
			wg.Add(1)
			go func() {
				select2(c1, c2, out, stop)
				wg.Done()
			}()
			chans = chans[2:]
		default:
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
