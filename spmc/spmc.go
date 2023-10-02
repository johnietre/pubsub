package spmc

// TODO: Change chanCap to chanCap

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnietre/pubsub/utils"
)

var (
	// ErrCantPub is returned when a client can't publish to a channel (doesn't
	// own it).
	ErrCantPub = fmt.Errorf("client can't publish on channel")
	// ErrChanClosed is returned when an operation is attempted on a closed
	// channel.
	ErrChanClosed = fmt.Errorf("channel closed")
	// ErrChanExists is returned when trying to create a channel that already
	// exists.
	ErrChanExists = fmt.Errorf("channel already exists")
	// ErrChanNotExist is returned when trying to access a non-existent channel.
	ErrChanNotExist = fmt.Errorf("channel doesn't exist")
	// ErrClientChanClosed is returned when the client's message chan is closed
	// and some sort of operation is done on it (not all methods using the
	// client's chan can return this).
	ErrClientChanClosed = fmt.Errorf("client channel closed")
	// ErrClientClosed is returned when an operation is attempted on a closed
	// client.
	ErrClientClosed = fmt.Errorf("client closed")
	// ErrClientSubbedAll is returned when trying to get a sub channel and the
	// client is subbed to all.
	ErrClientSubbedAll = fmt.Errorf("client is subbed to all")
	// ErrNotSubbed is returned when a client tries to get a channel it's not
	// subbed to.
	ErrNotSubbed = fmt.Errorf("not subbed")
	// ErrTimedOut is returned when an operation times out.
	ErrTimedOut = fmt.Errorf("timed out")
)

// Netowrk is a network of channels that clients can pub/sub to.
type Network[T any] struct {
	channels *utils.SyncMap[string, *channel[T]]
	// Holds the clients that were created to be subbed. If a client unsubs from
	// all, they are removed from this list. If a client subs to all after not
	// being subbed to all, there are placed in postSubAllClients
	subAllClients     *utils.SyncSet[*Client[T]]
	postSubAllClients *utils.SyncSet[*Client[T]]
}

// NetNetwork creates a new network.
func NewNetwork[T any]() *Network[T] {
	return &Network[T]{
		channels:          utils.NewSyncMap[string, *channel[T]](),
		subAllClients:     utils.NewSyncSet[*Client[T]](),
		postSubAllClients: utils.NewSyncSet[*Client[T]](),
	}
}

// NewClient creates a new client on the network with the given chan capacity.
func (n *Network[T]) NewClient(chanCap uint32) *Client[T] {
	return &Client[T]{
		network: n,
		chanCap: chanCap,
		msgChan: make(chan ChannelMsg[T], chanCap),
	}
}

// NewClientSubCurrent creates a new client subbed to the current channels.
func (n *Network[T]) NewClientSubCurrent(chanCap uint32) *Client[T] {
	c := &Client[T]{
		network: n,
		chanCap: chanCap,
		msgChan: make(chan ChannelMsg[T], chanCap),
	}
	n.channels.Range(func(_ string, ch *channel[T]) bool {
		ch.subs.Insert(c)
		return true
	})
	return c
}

// NewClientSubAll creates a new client subbed to all current and future
// channels.
func (n *Network[T]) NewClientSubAll(chanCap uint32) *Client[T] {
	c := &Client[T]{
		network: n,
		chanCap: chanCap,
		msgChan: make(chan ChannelMsg[T], chanCap),
	}
	c.subbedAll.Store(true)
	c.subAllSet.Store(n.subAllClients)
	n.subAllClients.Insert(c)
	return c
}

// ChanNames returns the names of all the channels curently on the network.
// If possible, the program should globally cache the names if they will be
// needed more than once since the names are regenerated on each call.
func (n *Network[T]) ChanNames() []string {
	var names []string
	n.channels.Range(func(name string, _ *channel[T]) bool {
		names = append(names, name)
		return true
	})
	return names
}

// HasChannel returns true if the network has a channel with the given name.
func (n *Network[T]) HasChannel(name string) bool {
	_, loaded := n.channels.Load(name)
	return loaded
}

func (n *Network[T]) addChannel(ch *channel[T]) (PubChannel[T], error) {
	if _, loaded := n.channels.LoadOrStore(ch.name, ch); loaded {
		return nil, ErrChanExists
	}
	// TODO: Possibly add field to clients to notify on channel creation
	n.postSubAllClients.Range(func(c *Client[T]) bool {
		ch.subs.Insert(c)
		return true
	})
	return newPubChannel(ch), nil
}

// Client is a client on a network.
type Client[T any] struct {
	network *Network[T]

	chanCap uint32
	msgChan chan ChannelMsg[T]
	// Messages should only be sent on the channel if the read or write lock is
	// held.
	chanMtx sync.RWMutex

	// TODO: If subbed to all, possibly keep set of channels to exclude/ignore
	subbedAll atomic.Bool
	subAllSet atomic.Pointer[utils.SyncSet[*Client[T]]]

	isClosed atomic.Bool
}

// Network returns the client's network.
func (c *Client[T]) Network() *Network[T] {
	return c.network
}

// Chan returns the client's chan that it receives channel messages on. When
// receiving on the chan, the receiver should check for both the message and
// the channel closure (i.e., `msg, ok := <-Client.Chan()`) since the channel
// will be closed on client closure.
func (c *Client[T]) Chan() <-chan ChannelMsg[T] {
	return c.msgChan
}

// TryRecv tries to receive a message on the clients chan, returning false if
// there's no message to be received.
func (c *Client[T]) TryRecv() (msg ChannelMsg[T], ok bool) {
	select {
	case msg, ok = <-c.msgChan:
	default:
	}
	return
}

// RecvTimeout tries to receive a message during the given duration, returning
// ErrTimedOut on timeout.
func (c *Client[T]) RecvTimeout(
	dur time.Duration,
) (msg ChannelMsg[T], err error) {
	timer, ok := time.NewTimer(dur), false
	select {
	case msg, ok = <-c.msgChan:
		if !ok {
			err = ErrClientChanClosed
		}
		timer.Stop()
	case <-timer.C:
		err = ErrTimedOut
	}
	return
}

// ResizeChan attempts to resize the client's message chan and transfer over.
// The old messages into the new chan. If the new capacity is less than the
// current, the difference is dropped.
// This should not be used often, especially if the current chan capacity is
// sufficiently large and there are a sufficient number of messages queued
// since this locks the client until all messages are transferred, which slows
// down all other operations that involve the client (like when a channel pubs
// and the client is a sub).
func (c *Client[T]) ResizeChan(chanCap uint32) error {
	if c.isClosed.Load() {
		return ErrClientClosed
	}
	c.chanMtx.Lock()
	defer c.chanMtx.Unlock()
	if c.IsClosed() {
		return ErrClientClosed
	}
	//close(c.msgChan)
	newCh := make(chan ChannelMsg[T], chanCap)
ResizeChanLoop:
	for i := uint32(0); i < chanCap; i++ {
		select {
		case msg := <-c.msgChan:
			select {
			case newCh <- msg:
			default:
				break ResizeChanLoop
			}
		default:
			break ResizeChanLoop
		}
	}
	close(c.msgChan)
	c.msgChan = newCh
	c.chanCap = chanCap
	return nil
}

// NewSub attempts to subscribe to a channel. Returns ErrClientSubbedAll if the
// client is currently subbed to all channels.
func (c *Client[T]) NewSub(name string) (SubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	} else if c.IsSubbedAll() {
		return nil, ErrClientSubbedAll
	}
	ch, loaded := c.network.channels.Load(name)
	if !loaded {
		return nil, ErrChanNotExist
	} else if !c.IsSubbedAll() && !ch.subs.Contains(c) {
		ch.subs.Insert(c)
	}
	return newSubChannel(ch), nil
}

// Sub is an alias for Client.NewSub.
func (c *Client[T]) Sub(name string) (SubChannel[T], error) {
  return c.NewSub(name)
}

// NewSubs attempts to subscribe the the channels with the given names. Returns
// basically the same errors/error structure as GetSubs.
func (c *Client[T]) NewSubs(names ...string) ([]SubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	} else if c.IsSubbedAll() {
		return nil, ErrClientSubbedAll
	}
	subs := make([]SubChannel[T], 0, len(names))
	var errs utils.ErrorSlice
	for i, name := range names {
		if ch, err := c.NewSub(name); err != nil {
			errs = append(errs, utils.ElemError{Index: i, Err: err})
		} else {
			subs = append(subs, ch)
		}
	}
	if len(errs) == 0 {
		return subs, nil
	}
	return subs, errs
}

// SubCurrent attempts to subscribe to all the channels currently on the
// network. Doesn't subscribe to future channels.
func (c *Client[T]) SubCurrent() error {
	if c.isClosed.Load() {
		return ErrClientClosed
	} else if c.IsSubbedAll() {
		return ErrClientSubbedAll
	}
	c.subCurrent()
	return nil
}

func (c *Client[T]) subCurrent() {
	c.network.channels.Range(func(_ string, ch *channel[T]) bool {
		ch.subs.Insert(c)
		return true
	})
}

// SubAll attempts to sub to all channels. Does nothing if the client is
// already subbed to all.
func (c *Client[T]) SubAll() error {
	if c.isClosed.Load() {
		return ErrClientClosed
	} else if c.IsSubbedAll() {
		return nil
	}
	psac := c.network.postSubAllClients
	psac.Insert(c)
	c.subAllSet.Store(psac)
	c.subCurrent()
	return nil
}

// GetSub attempts to get the channel with the given name if the client is
// subbed to it. Returns ErrClientSubbedAll if the client is currently subbed
// to all channels.
func (c *Client[T]) GetSub(name string) (SubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	} else if c.IsSubbedAll() {
		// TODO: Return channel anyway?
		return nil, ErrClientSubbedAll
	}
	ch, loaded := c.network.channels.Load(name)
	if !loaded {
		return nil, ErrChanNotExist
	} else if !ch.subs.Contains(c) {
		return nil, ErrNotSubbed
	}
	return newSubChannel(ch), nil
}

// GetSubs attempts to get the channels with the given names if the client is
// subbed to them. Returns ErrClientSubbedAll if the client is currently subbed
// to all channels. If there are any errors getting any of the names passed,
// a utils.ErrorSlice is returned with each error of the slice being a
// util.ElemError set with the error and index of the name in the passed names.
func (c *Client[T]) GetSubs(names ...string) ([]SubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	} else if c.IsSubbedAll() {
		// TODO: Don't return error if subAllSet == postSub...?
		return nil, ErrClientSubbedAll
	}
	subs := make([]SubChannel[T], 0, len(names))
	var errs utils.ErrorSlice
	for i, name := range names {
		if ch, err := c.GetSub(name); err != nil {
			errs = append(errs, utils.ElemError{Index: i, Err: err})
		} else {
			subs = append(subs, ch)
		}
	}
	if len(errs) == 0 {
		return subs, nil
	}
	return subs, errs
}

// GetAllSubs attempts to get all the channels the client is currently subbed
// to. Returns ErrClientSubbedAll if the client is currently subbed to all
// channels.
func (c *Client[T]) GetAllSubs() ([]SubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	} else if c.IsSubbedAll() {
		return nil, ErrClientSubbedAll
	}
	var subs []SubChannel[T]
	c.network.channels.Range(func(_ string, ch *channel[T]) bool {
		if ch.subs.Contains(c) {
			subs = append(subs, newSubChannel(ch))
		}
		return true
	})
	return subs, nil
}

// IsSubbedAll returns true if the client is currently subbed to all channels.
func (c *Client[T]) IsSubbedAll() bool {
	return c.subAllSet.Load() != nil
}

// Unsub attempts to unsub from the given channels. For element-wise errors
// (errors with certain names passed, a similar error structure to GetSubs is
// returned). Returns ErrClientSubbedAll if the client is currently subbed to
// all channels.
func (c *Client[T]) Unsub(names ...string) error {
	if c.isClosed.Load() {
		return ErrClientClosed
	} else if set := c.subAllSet.Load(); set == c.network.subAllClients {
		return ErrClientSubbedAll
	}
	var errs utils.ErrorSlice
	for i, name := range names {
		ch, loaded := c.network.channels.Load(name)
		if !loaded {
			errs = append(errs, utils.NewElemError(i, ErrChanNotExist))
			continue
		}
		if err := ch.unsub(c); err != nil {
			errs = append(errs, utils.NewElemError(i, err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// UnsubAll attempts to unsub from all channels. If the client is subbed to
// all, this will unsub them from all (IsSubbedAll() == false). Note, calling
// unsub can be extremely inefficient since it must go through all channels in
// most cases.
func (c *Client[T]) UnsubAll() error {
	if c.isClosed.Load() {
		return ErrClientClosed
	}
	c.unsubAll()
	return nil
}

func (c *Client[T]) unsubAll() {
	if set := c.subAllSet.Swap(nil); set == c.network.subAllClients {
		c.network.subAllClients.Remove(c)
		return
	} else if set == c.network.postSubAllClients {
		c.network.postSubAllClients.Remove(c)
		// No return since client subbed to channels explicitly if in postSub...
	}
	c.network.channels.Range(func(_ string, ch *channel[T]) bool {
		ch.unsub(c)
		return true
	})
}

// NewPub attempts to create a new channel with the client as the publisher.
func (c *Client[T]) NewPub(
	name string, msgOnClose bool,
) (PubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	}
	ch := &channel[T]{
		network:         c.network,
		pubClient:       c,
		name:            name,
		subs:            utils.NewSyncSet[*Client[T]](),
		sendsMsgOnClose: msgOnClose,
	}
	return c.network.addChannel(ch)
}

// NewPubs attempts to create new channels with the given names, all with the
// given msgOnClose. Returns basically the same errors/error structure as
// GetPubs.
func (c *Client[T]) NewPubs(msgOnClose bool, names ...string) ([]PubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	}
	pubs := make([]PubChannel[T], 0, len(names))
	var errs utils.ErrorSlice
	for i, name := range names {
		if ch, err := c.NewPub(name, msgOnClose); err != nil {
			errs = append(errs, utils.ElemError{Index: i, Err: err})
		} else {
			pubs = append(pubs, ch)
		}
	}
	if len(errs) == 0 {
		return pubs, nil
	}
	return pubs, errs
}

// GetPub attempts to get a channel the client owns (and can publish to).
func (c *Client[T]) GetPub(name string) (PubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	}
	ch, loaded := c.network.channels.Load(name)
	if !loaded {
		return nil, ErrChanNotExist
	}
	if ch.pubClient != c {
		return nil, ErrCantPub
	}
	return newPubChannel(ch), nil
}

// GetPubs attempts to get the channels with the given names. Element-wise are
// returned in the same fashion as GetSubs.
func (c *Client[T]) GetPubs(names ...string) ([]PubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	}
	pubs := make([]PubChannel[T], 0, len(names))
	var errs utils.ErrorSlice
	for i, name := range names {
		if ch, err := c.GetPub(name); err != nil {
			errs = append(errs, utils.ElemError{Index: i, Err: err})
		} else {
			pubs = append(pubs, ch)
		}
	}
	if len(errs) == 0 {
		return pubs, nil
	}
	return pubs, errs
}

// GetAllPubs attempts to get all channels the client currently owns.
func (c *Client[T]) GetAllPubs() ([]PubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	}
	var pubs []PubChannel[T]
	c.network.channels.Range(func(_ string, ch *channel[T]) bool {
		if ch.pubClient == c {
			pubs = append(pubs, newPubChannel(ch))
		}
		return true
	})
	return pubs, nil
}

// Pub attempts to publish the given value to the channel with the given name.
func (c *Client[T]) Pub(name string, val T) (PubChannel[T], error) {
	if c.isClosed.Load() {
		return nil, ErrClientClosed
	}
	ch, err := c.GetPub(name)
	if err != nil {
		return nil, err
	}
	return ch, ch.Pub(val)
}

// ClosePub attempts to close the channel with the given name. Returns
// ErrCantPub if the client can't publish to (doesn't own) the channel.
func (c *Client[T]) ClosePub(name string) error {
	if c.isClosed.Load() {
		return ErrClientClosed
	}
	ch, err := c.GetPub(name)
	if err != nil {
		return err
	}
	return ch.Close()
}

// ClosePubs attempts to close the given channels. Returns similar errors
// to GetPubs.
func (c *Client[T]) ClosePubs(names ...string) error {
	if c.isClosed.Load() {
		return ErrClientClosed
	}
	var errs utils.ErrorSlice
	for i, name := range names {
		if err := c.ClosePub(name); err != nil {
			errs = append(errs, utils.ElemError{Index: i, Err: err})
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// CloseAllPubs attempts to close all channels the client owns.
func (c *Client[T]) CloseAllPubs() error {
	if c.isClosed.Load() {
		return ErrClientClosed
	}
	c.closeAllPubs()
	return nil
}

func (c *Client[T]) closeAllPubs() {
	c.network.channels.Range(func(_ string, ch *channel[T]) bool {
		if ch.pubClient == c {
			ch.close()
		}
		return true
	})
	return
}

// IsClosed returns true if the client has been closed.
func (c *Client[T]) IsClosed() bool {
	return c.isClosed.Load()
}

// Close closes the client by closing it's message chan, closing all it's
// owned channels, and unsubbing from all it's subbed channels.
func (c *Client[T]) Close() error {
	// TODO: Should the lock be held here?
	if c.isClosed.Swap(true) {
		return ErrClientClosed
	}
	// These are called since their exported counterpoarts won't run if the
	// client is closed
	c.closeAllPubs()
	c.unsubAll()
	c.chanMtx.Lock()
	defer c.chanMtx.Unlock()
	close(c.msgChan)
	return nil
}

func (c *Client[T]) send(msg ChannelMsg[T]) {
	if c.isClosed.Load() {
		return
	}
	c.chanMtx.RLock()
	if !c.isClosed.Load() {
		select {
		case c.msgChan <- msg:
		default:
		}
	}
	c.chanMtx.RUnlock()
}

type channel[T any] struct {
	network         *Network[T]
	pubClient       *Client[T]
	name            string
	subs            *utils.SyncSet[*Client[T]]
	chanIsClosed    atomic.Bool
	sendsMsgOnClose bool
}

func (ch *channel[T]) pub(val T) error {
	if ch.chanIsClosed.Load() {
		return ErrChanClosed
	}
	msg := newChannelMsg(ch, val)
	ch.subs.Range(func(c *Client[T]) bool {
		c.send(msg)
		return true
	})
	ch.network.subAllClients.Range(func(c *Client[T]) bool {
		c.send(msg)
		return true
	})
	return nil
}

func (ch *channel[T]) unsub(c *Client[T]) error {
	// TODO: Necessary?
	if ch.chanIsClosed.Load() {
		return ErrChanClosed
	}
	if !ch.subs.Remove(c) {
		return ErrNotSubbed
	}
	return nil
}

func (ch *channel[T]) msgOnClose() bool {
	return ch.sendsMsgOnClose
}

func (ch *channel[T]) isClosed() bool {
	return ch.chanIsClosed.Load()
}

func (ch *channel[T]) close() error {
	if ch.chanIsClosed.Swap(true) {
		return ErrChanClosed
	}
	ch.network.channels.Delete(ch.name)
	if ch.sendsMsgOnClose {
		msg := ChannelMsg[T]{channel: newSubChannel(ch), isClose: true}
		ch.subs.Range(func(c *Client[T]) bool {
			c.send(msg)
			ch.subs.Remove(c)
			return true
		})
		ch.network.subAllClients.Range(func(c *Client[T]) bool {
			c.send(msg)
			return true
		})
	} else {
		// TODO: Necessary?
		ch.subs.Range(func(c *Client[T]) bool {
			ch.subs.Remove(c)
			return true
		})
	}
	return nil
}

// PubChannel represents a channel that can be published to.
type PubChannel[T any] interface {
	// Name returns the name of the channel.
	Name() string
	// MsgOnClose returns true if the channel sends a message on closure.
	MsgOnClose() bool
	// Pub attempts to publish to the channel.
	Pub(T) error
	// Close attempts to close the channel.
	Close() error
	// IsClosed returns true if the channel is closed.
	IsClosed() bool
}

type pubChannel[T any] struct {
	ch *channel[T]
}

func newPubChannel[T any](ch *channel[T]) *pubChannel[T] {
	return &pubChannel[T]{ch}
}

// Name implements the PubChannel interface.
func (pc *pubChannel[T]) Name() string {
	return pc.ch.name
}

// MsgOnClose implements the PubChannel interface.
func (pc *pubChannel[T]) MsgOnClose() bool {
	return pc.ch.msgOnClose()
}

// Pub implements the PubChannel interface.
func (pc *pubChannel[T]) Pub(val T) error {
	return pc.ch.pub(val)
}

// Close implements the PubChannel interface.
func (pc *pubChannel[T]) Close() error {
	return pc.ch.close()
}

// IsClosed implements the PubChannel interface.
func (pc *pubChannel[T]) IsClosed() bool {
	return pc.ch.isClosed()
}

// SubChannel represents a channel that has been subscribed to.
type SubChannel[T any] interface {
	// Name returns the name of the channel.
	Name() string
	// MsgOnClose returns true if the channel sends a message on closure.
	MsgOnClose() bool
	// TODO: Have subChannel hold client addr and not take client for Unsub?
	// Unsub attempts to unsubcribe the client from the channel
	Unsub(*Client[T])
	// IsClosed returns true if the channel is closed.
	IsClosed() bool
}

type subChannel[T any] struct {
	ch *channel[T]
}

func newSubChannel[T any](ch *channel[T]) *subChannel[T] {
	return &subChannel[T]{ch}
}

// Name implements the SubChannel interface.
func (sc *subChannel[T]) Name() string {
	return sc.ch.name
}

// MsgOnClose implements the SubChannel interface.
func (sc *subChannel[T]) MsgOnClose() bool {
	return sc.ch.msgOnClose()
}

// Unsub implements the SubChannel interface.
func (sc *subChannel[T]) Unsub(c *Client[T]) {
	sc.ch.unsub(c)
}

// IsClosed implements the SubChannel interface.
func (sc *subChannel[T]) IsClosed() bool {
	return sc.ch.isClosed()
}

// ChannelMsg is a message sent/received on a channel.
type ChannelMsg[T any] struct {
	channel SubChannel[T]
	val     T
	isClose bool
}

func newChannelMsg[T any](ch *channel[T], val T) ChannelMsg[T] {
	return ChannelMsg[T]{
		channel: &subChannel[T]{ch},
		val:     val,
	}
}

// Channel returns the channel that sent the message.
func (cm *ChannelMsg[T]) Channel() SubChannel[T] {
	return cm.channel
}

// Value returns the value of the message.
func (cm *ChannelMsg[T]) Value() T {
	return cm.val
}

// IsClose returns true if the message represents a channel closure.
func (cm *ChannelMsg[T]) IsClose() bool {
	return cm.isClose
}
