package utils

import (
  "errors"
  "sync"
)

type MsgType byte

const (
  MsgTypeErr MsgType = iota
	MsgTypeSub
	MsgTypeUnsub
	MsgTypePub
  MsgTypeNewChan
  MsgTypeNewMultiChan
  MsgTypeDelChan
  MsgTypeChanNames
  MsgTypeOk
)

var (
  ErrBadMsg = errors.New("malformed message")
  ErrMismatchLen = errors.New("mismatch content length")
)

func EncodeMsg(mt MsgType, contents string) []byte {
	return append([]byte{byte(mt)}, append(Put4(uint32(len(contents))), contents...)...)
}

func DecodeMsg(msg []byte) (MsgType, string, error) {
  l := len(msg)
  if l < 5 {
    return MsgTypeErr, "", ErrBadMsg
  }
  mt := MsgType(msg[0])
  cl := int(Get4(msg[1:5]))
  if cl + 5 != l {
    return MsgTypeErr, "", ErrMismatchLen
  }
  return mt, string(msg[5:5+cl]), nil
}

func Put4(u uint32) []byte {
	return []byte{
		byte(u << 24),
		byte(u << 16),
		byte(u << 8),
		byte(u),
	}
}

func Get4(b []byte) uint32 {
	return uint32(b[0]>>24) | uint32(b[1]>>16) | uint32(b[2]>>8) | uint32(b[3])
}

type SyncMap[K any, V any] struct {
  m sync.Map
}

func NewSyncMap[K any, V any]() *SyncMap[K, V] {
  return &SyncMap[K, V]{}
}

func (sm *SyncMap[K, V]) Load(key K) (v V, loaded bool) {
  var val any
  val, loaded = sm.m.Load(key)
  if loaded {
    v = val.(V)
  }
  return
}

func (sm *SyncMap[K, V]) LoadOrStore(key K, val V) (v V, loaded bool) {
  var value any
  value, loaded = sm.m.LoadOrStore(key, val)
  return value.(V), loaded
}

func (sm *SyncMap[K, V]) LoadAndDelete(key K) (v V, loaded bool) {
  var val any
  val, loaded = sm.m.LoadAndDelete(key)
  if loaded {
    v = val.(V)
  }
  return
}

func (sm *SyncMap[K, V]) Delete(key K) {
  sm.m.Delete(key)
}

func (sm *SyncMap[K, V]) Range(f func(K, V) bool) {
  sm.m.Range(func(key, val any) bool {
    return f(key.(K), val.(V))
  })
}

// Set represents a hash set
type Set[T comparable] struct {
  m map[T]Unit
}

// NewSet creates a new Set
func NewSet[T comparable]() *Set[T] {
  return &Set[T]{m: make(map[T]Unit)}
}

// Insert inserts a value, returning true if the value didn't exist
func (s *Set[T]) Insert(item T) bool {
  if _, ok := s.m[item]; !ok {
    s.m[item] = Unit{}
    return true
  }
  return false
}

// Remove deletes a value, returning true if the value existed
func (s *Set[T]) Remove(item T) bool {
  if _, ok := s.m[item]; !ok {
    delete(s.m, item)
    return true
  }
  return false
}

// Contains returns whether the set contains the item
func (s *Set[T]) Contains(item T) bool {
  _, ok := s.m[item]
  return ok
}

// Range iterates over each item in random order, applying a given function
// that returns whether the iterations should stop
func (s *Set[T]) Range(f func(T) bool) {
  for item := range s.m {
    if !f(item) {
      return
    }
  }
}

// SyncSet represents a hash set using sync.Map
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

// Unit is an empty struct (struct{})
type Unit struct{}
