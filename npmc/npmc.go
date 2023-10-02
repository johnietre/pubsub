package npmc

// TODO: Fix deadlock when closing channel between Close, cloneWithClient,
// Channel.Unsub, etc.

// TODO: MULTIPUB
// TODO: Return errors instead of bools?

import (
	"sync"
	"sync/atomic"

	"github.com/johnietre/pubsub/utils"
)

// Netowrk is a network of channels that clients can pub/sub to.
type Network[T any] struct {
	channels *utils.SyncMap[string, *Channel[T]]
	// Holds the client and the chanLen passed
	subAllClients *utils.SyncMap[*Client[T], uint32]
}

// NetNetwork creates a new network.
func NewNetwork[T any]() *Network[T] {
	return &Network[T]{
		channels:      utils.NewSyncMap[string, *Channel[T]](),
		subAllClients: utils.NewSyncMap[*Client[T], uint32](),
	}
}

// NewClient creates a new client on the network.
func (n *Network[T]) NewClient() *Client[T] {
	return &Client[T]{
		network: n,
		pubs:    utils.NewSyncMap[string, *Channel[T]](),
		subs:    utils.NewSyncMap[string, *Channel[T]](),
	}
}

// NewClient creates a new client on the network that doesn't keep track of the
// channels it's subbed to.
func (n *Network[T]) NewClientLite() *Client[T] {
	return &Client[T]{
		network: n,
		pubs:    utils.NewSyncMap[string, *Channel[T]](),
		subs:    utils.NewSyncMap[string, *Channel[T]](),
		isLite:  true,
	}
}

// NewClientSubCurrent creates a new client that's subbed to all of the current
// channels.
func (n *Network[T]) NewClientSubCurrent(lite bool, chanLen uint32) *Client[T] {
	c := &Client[T]{
		network: n,
		pubs:    utils.NewSyncMap[string, *Channel[T]](),
		subs:    utils.NewSyncMap[string, *Channel[T]](),
		isLite:  lite,
	}
	n.channels.Range(func(name string, ch *Channel[T]) bool {
		c.NewSub(name, chanLen)
		return true
	})
	return c
}

// NewClientSubAll creates a new client that's subbed to all of the current
// channels and will auto-sub to all future channels.
func (n *Network[T]) NewClientSubAll(lite bool, chanLen uint32) *Client[T] {
	c := &Client[T]{
		network: n,
		pubs:    utils.NewSyncMap[string, *Channel[T]](),
		subs:    utils.NewSyncMap[string, *Channel[T]](),
		isLite:  lite,
	}
	n.subAllClients.Store(c, chanLen)
	n.channels.Range(func(name string, ch *Channel[T]) bool {
		c.NewSub(name, chanLen)
		return true
	})
	return c
}

// RangeNames iterates through the names of the channels on the network and
// passes each to the provided function, `f`. If `f` returns false, iteration
// stops.
func (n *Network[T]) RangeNames(f func(string) bool) {
	n.channels.Range(func(name string, _ *Channel[T]) bool {
		return f(name)
	})
}

// ListNames returns a list of the current channel names on the network.
// NOTE: This creates a new list of the channel names each call, so the
// returned list can be modified, but, of course, has the downside of a new
// list being created every call.
func (n *Network[T]) ListNames() []string {
	var names []string
	n.channels.Range(func(name string, _ *Channel[T]) bool {
		names = append(names, name)
		return true
	})
	return names
}

// Creates a new pub channel if the channel doesn't already exist. If it does,
// it returns false and the channel (if the client owns the channel) or nil (if
// the client doesn't). Additionally, nil, false is returned if the client is
// closed. This only tries to create a singlepub channel. If the channel exists
// but is multipub, nil, false is returned no matter what.
func (n *Network[T]) addChannel(
	client *Client[T], ch *Channel[T],
) (*Channel[T], bool) {
	ch, loaded := n.channels.LoadOrStore(ch.name, ch)
	if loaded {
		if ch.client != client || ch.multiplePubs {
			ch = nil
		}
	} else {
		// loaded shouldn't be true but check anyway
		if ch, loaded := client.pubs.LoadOrStore(ch.name, ch); loaded {
			return ch, false
		}
		n.subAllClients.Range(func(c *Client[T], chanLen uint32) bool {
			c.NewSub(ch.name, chanLen)
			return true
		})
	}
	return ch, !loaded
}

// Client represents a client on network that can publish (create) and
// subscribe to channels.
type Client[T any] struct {
	network  *Network[T]
	pubs     *utils.SyncMap[string, *Channel[T]]
	subs     *utils.SyncMap[string, *Channel[T]]
	isLite   bool
	isClosed atomic.Bool
	// This is used to help syncronize when the channel is closed
	closeMtx sync.RWMutex
}

// Network returns the network the client is associated with.
func (c *Client[T]) Network() *Network[T] {
	return c.network
}

// IsLite returns true if the client was created with Network.NewClientLite.
func (c *Client[T]) IsLite() bool {
	return c.isLite
}

// NewPub creates a new pub channel if the channel doesn't already exist. If it
// does, it returns false and the channel (if the client owns the channel) or
// nil (if the client doesn't). Additionally, nil, false is returned if the
// client is closed. This only tries to create a singlepub channel. If the
// channel exists but is multipub, nil, false is returned no matter what.
func (c *Client[T]) NewPub(name string) (*Channel[T], bool) {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil, false
	}
	ch := &Channel[T]{
		client:   c,
		network:  c.network,
		name:     name,
		isPub:    true,
		isClosed: &atomic.Bool{},
		subChans: utils.NewSyncMap[*Client[T], chan ChannelMsg[T]](),
		pubCount: &atomic.Int32{},
		chanMtx:  &sync.RWMutex{},
	}
	ch.pubCount.Add(1)
	return c.network.addChannel(c, ch)
}

/* TODO: MULTIPUB
// NewMultiPub creates a new pub channel than can have multiple pubs, if it
// doesn't already exist. If it didn't exist, the channel and true is returned.
// If the channel existed and is multipub, returns the channel and false. If
// the channel exists but isn't multipub, or the client is closed, nil, false is
// returned. Also returned if the channel is closed (no more pubs can be added).
func (c *Client[T]) NewMultiPub(name string) (*Channel[T], bool) {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil, false
	}
	ch := &Channel[T]{
		client:   c,
		network:  c.network,
		name:     name,
		isPub:    true,
		isClosed: &atomic.Bool{},
    multiplePubs: true,
		subChans: utils.NewSyncMap[*Client[T], chan ChannelMsg[T]](),
		chanMtx:  &sync.RWMutex{},
	}
	ch, loaded := c.network.channels.LoadOrStore(name, ch)
	if loaded {
    if !ch.multiplePubs || !ch.incrPubs(c) {
      return nil, false
    }
	}
	return ch, !loaded
}
*/

// GetPub returns the pub channel with the given name if it exists.
func (c *Client[T]) GetPub(name string) *Channel[T] {
	ch, _ := c.pubs.Load(name)
	return ch
}

// ClosePub closes the pub channel with the given name, if it exists and the
// client is the publisher.
func (c *Client[T]) ClosePub(name string) {
	if ch, loaded := c.pubs.Load(name); loaded {
		ch.Close()
	}
}

// ClosePubs closes all channels the client publishes on.
func (c *Client[T]) ClosePubs() {
	c.pubs.Range(func(name string, _ *Channel[T]) bool {
		c.ClosePub(name)
		return true
	})
}

// NewSub subscribes to a channel and returns it, returning nil, false if it
// doesn't exist/is closed. If the client is already subscribed to the channel,
// a new instance of Channel is created with the existing chan (meaning chanLen
// may not be used) and the channel, false is returned. Additionally, nil,
// false is returned if the channel is closed. Therefore, the channel is
// returned with true if the client was not already subbed to the channel.
func (c *Client[T]) NewSub(name string, chanLen uint32) (*Channel[T], bool) {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil, false
	}
	if c.isLite {
		return c.newSubLite(name, chanLen)
	}
	if ch, loaded := c.subs.Load(name); loaded {
		return ch, true
	}
	ch, loaded := c.network.channels.Load(name)
	if !loaded {
		return nil, false
	}
	subChannel, justSubbed := ch.newSub(c, chanLen)
	if subChannel == nil {
		return nil, false
	} else if !justSubbed {
		// Don't store if this wasn't the channel wasn't just subbed to
		return subChannel, false
	}
	c.subs.Store(name, subChannel)
	return subChannel, true
}

func (c *Client[T]) newSubLite(
	name string, chanLen uint32,
) (ch *Channel[T], subbed bool) {
	c.network.channels.Range(func(chName string, nCh *Channel[T]) bool {
		if chName == name {
			ch, subbed = nCh.newSub(c, chanLen)
			return false
		}
		return true
	})
	return nil, false
}

// GetSub returns the sub channel with the given name if it exists.
func (c *Client[T]) GetSub(name string) *Channel[T] {
	if c.isLite {
		return c.getSubLite(name)
	}
	ch, _ := c.subs.Load(name)
	return ch
}

func (c *Client[T]) getSubLite(name string) *Channel[T] {
	if nCh, loaded := c.network.channels.Load(name); loaded {
		return nCh.cloneWithClient(c)
	}
	return nil
}

// Unsub unsubscribes from a channel.
func (c *Client[T]) Unsub(name string) {
	c.unsub(name, true)
}

// True should be passed if the channel should hold the chanMtx read lock when
// unsubbing. See Channel.unsub.
func (c *Client[T]) unsub(name string, lock bool) {
	if c.isLite {
		c.unsubLite(name, lock)
		return
	}
	if ch, loaded := c.subs.Load(name); loaded {
		ch.unsub(lock)
	}
}

// For lock param, see Client.unsub.
func (c *Client[T]) unsubLite(name string, lock bool) {
	if nCh, loaded := c.network.channels.Load(name); loaded {
		if ch := nCh.cloneWithClient(c); ch != nil {
			ch.unsub(lock)
		}
	}
}

// UnsubAll unsubscribes from all channels.
func (c *Client[T]) UnsubAll() {
	if c.isLite {
		c.unsubAllLite()
		return
	}
	c.subs.Range(func(name string, _ *Channel[T]) bool {
		c.Unsub(name)
		return true
	})
}

func (c *Client[T]) unsubAllLite() {
	c.network.channels.Range(func(chName string, nCh *Channel[T]) bool {
		if _, loaded := nCh.subChans.Load(c); loaded {
			if ch := nCh.cloneWithClient(c); ch != nil {
				ch.Unsub()
			}
		}
		return true
	})
}

// StartSubAll makes the client sub to all new channels that are created with
// the given chanLen. If `replace` is true and the channel is already set to
// sub to all new channels, the chanLen used is replaced with the provided
// value. False is returned if the client was already set to sub to new.
func (c *Client[T]) StartSubAll(chanLen uint32, replace bool) bool {
	_, loaded := c.network.subAllClients.LoadOrStore(c, chanLen)
	if loaded {
		c.network.subAllClients.Store(c, chanLen)
	}
	return !loaded
}

// SubAllChanLen returns the length used when auto-subbing the client to newly
// created channels. Returns false if the client is not set to do this.
func (c *Client[T]) SubAllChanLen() (uint32, bool) {
	l, loaded := c.network.subAllClients.Load(c)
	return l, loaded
}

// StopSubAll stops the client from subbing to new channels that are created.
// Returns true if the client was set to do this.
func (c *Client[T]) StopSubAll() bool {
	_, loaded := c.network.subAllClients.LoadAndDelete(c)
	return !loaded
}

// Close closes the client, unsubscribing (sub) and closing (pub) all channels.
func (c *Client[T]) Close() {
	// Avoid an unnecessary locking if the channel is already closed.
	if c.isClosed.Swap(true) {
		return
	}
	c.closeMtx.Lock()
	c.StopSubAll()
	c.UnsubAll()
	c.ClosePubs()
	c.closeMtx.Unlock()
}

// MergePubs merges the client's pub channels with the given names into one
// merged channel to do mass publishing on.
func (c *Client[T]) MergePubs(names ...string) *MergedChan[T] {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil
	}
	channels := utils.NewSyncMap[string, *Channel[T]]()
	for _, name := range names {
		if ch, loaded := c.pubs.Load(name); loaded {
			channels.Store(name, ch)
		}
	}
	return &MergedChan[T]{
		client:   c,
		channels: channels,
		isPub:    true,
	}
}

// MergePubChans merges the given pub channels into one merged channel to do
// mass publishing on.
func (c *Client[T]) MergePubChans(pubChans ...*Channel[T]) *MergedChan[T] {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil
	}
	channels := utils.NewSyncMap[string, *Channel[T]]()
	var chans []<-chan ChannelMsg[T]
	for _, ch := range pubChans {
		if ch.client == c && ch.isPub && !ch.IsClosed() {
			channels.Store(ch.name, ch)
			chans = append(chans, ch.Chan())
		}
	}
	return &MergedChan[T]{
		client:   c,
		channels: channels,
		isPub:    true,
	}
}

// MergePubs merges all the client's pub channels into one merged channel to do
// mass publishing on.
func (c *Client[T]) MergeAllPubs() *MergedChan[T] {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil
	}
	channels := utils.NewSyncMap[string, *Channel[T]]()
	c.pubs.Range(func(name string, ch *Channel[T]) bool {
		channels.Store(name, ch)
		return true
	})
	return &MergedChan[T]{
		client:   c,
		channels: channels,
		isPub:    true,
	}
}

// MergeSubs merges the client's sub channels with the given names into one
// merged channel to combine the outputs into one chan with a given length.
func (c *Client[T]) MergeSubs(chanLen uint32, names ...string) *MergedChan[T] {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil
	}
	channels := utils.NewSyncMap[string, *Channel[T]]()
	var chans []<-chan ChannelMsg[T]
	for _, name := range names {
		if ch := c.GetSub(name); ch != nil {
			channels.Store(name, ch)
			chans = append(chans, ch.Chan())
		}
	}
	out := make(chan ChannelMsg[T], chanLen)
	stopChan := utils.SelectN(out, chans...)
	return &MergedChan[T]{
		client:    c,
		channels:  channels,
		isPub:     false,
		out:       out,
		stopChans: []chan<- utils.Unit{stopChan},
	}
}

// MergeSubChans merges the given sub channels into one merged channel to
// combine the outputs into one chan with a given length.
func (c *Client[T]) MergeSubChans(chanLen uint32, subChans ...*Channel[T]) *MergedChan[T] {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil
	}
	channels := utils.NewSyncMap[string, *Channel[T]]()
	var chans []<-chan ChannelMsg[T]
	for _, ch := range subChans {
		if ch.client == c && !ch.isPub && !ch.IsClosed() {
			channels.Store(ch.name, ch)
			chans = append(chans, ch.Chan())
		}
	}
	out := make(chan ChannelMsg[T], chanLen)
	stopChan := utils.SelectN(out, chans...)
	return &MergedChan[T]{
		client:    c,
		channels:  channels,
		isPub:     false,
		out:       out,
		stopChans: []chan<- utils.Unit{stopChan},
	}
}

// MergeSubs merges all the client's sub channels into one merged channel to
// combine the outputs into one chan with a given length.
func (c *Client[T]) MergeAllSubs(chanLen uint32) *MergedChan[T] {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil
	}
	channels := utils.NewSyncMap[string, *Channel[T]]()
	var chans []<-chan ChannelMsg[T]
	if c.isLite {
		c.network.channels.Range(func(chName string, nCh *Channel[T]) bool {
			if _, loaded := nCh.subChans.Load(c); loaded {
				if ch := nCh.cloneWithClient(c); ch != nil {
					channels.Store(chName, ch)
					chans = append(chans, ch.Chan())
				}
			}
			return true
		})
	} else {
		c.subs.Range(func(name string, ch *Channel[T]) bool {
			channels.Store(name, ch)
			chans = append(chans, ch.Chan())
			return true
		})
	}
	out := make(chan ChannelMsg[T], chanLen)
	stopChan := utils.SelectN(out, chans...)
	return &MergedChan[T]{
		client:    c,
		channels:  channels,
		isPub:     false,
		out:       out,
		stopChans: []chan<- utils.Unit{stopChan},
	}
}

// MergedChan merges multiple channels inputs/outputs into one chan.
// Currently, there is no way to remove a single sub channel from the channels
// (and consequently the output chan) without closing said channel. If this is
// desired, right now, the entire MergedChan must be stopped (and messages
// should be exhausted from the output chan) and the MergedChan recreated while
// leaving out the desired channel(s). Pub channels can be removed, however,
// though removing a pub channel won't close it. Additionally, a closed sub
// chan won't be removed from the list; the only effect is no messages will be
// received from that channel on the output chan.
type MergedChan[T any] struct {
	client    *Client[T]
	channels  *utils.SyncMap[string, *Channel[T]]
	isPub     bool
	out       chan ChannelMsg[T]
	stopChan  chan<- utils.Unit
	isStopped atomic.Bool
	// No mutex is used since nothing is ever added to the struct fields; the
	// isStopped atomic can handle synchronization of closing stopChan.
	stopChansMtx sync.RWMutex
	stopChans    []chan<- utils.Unit
}

// Client returns the client associated with the MergedChan.
func (mc *MergedChan[T]) Client() *Client[T] {
	return mc.client
}

// Channels returns the list of channels associated with the merged chan.
// Nothing should be done with the returned map. Removing of pub chans should
// be done through the RemovePubs function.
func (mc *MergedChan[T]) Channels() *utils.SyncMap[string, *Channel[T]] {
	return mc.channels
}

// IsPub returns whether this represents a merging of pub channels.
func (mc *MergedChan[T]) IsPub() bool {
	return mc.isPub
}

// Chan returns the chan of the combined messages output. Returns nil if the
// merged chan is not a pub.
func (mc *MergedChan[T]) Chan() <-chan ChannelMsg[T] {
	return mc.out
}

// Pub publishes the given value to the channels if the merged chan is pub.
// Nothing is returned, even if all the channels are closed (nothing is done if
// this is the case).
func (mc *MergedChan[T]) Pub(t T) {
	if !mc.isPub {
		return
	}
	mc.channels.Range(func(_ string, ch *Channel[T]) bool {
		ch.Pub(t)
		return true
	})
}

// AddPubs adds more pub chans to the MergedChan, skipping if the chan is
// already a part of it. A channel will also not be added if it's client is not
// the same as the MergedChan's client, if it's not a pub channel, or if it is
// closed. Does nothing if the MergedChan is not pub.
func (mc *MergedChan[T]) AddPubs(pubChans ...*Channel[T]) {
	if !mc.isPub {
		return
	}
	for _, ch := range pubChans {
		if ch.client == mc.client && ch.isPub && !ch.IsClosed() {
			mc.channels.LoadOrStore(ch.name, ch)
		}
	}
}

// RemovePubs removes the specified channels from the list, if they exist.
// Removing a channel does not close it. Nothing is done if the merged channel
// is not pub (see type documentation for further explanation).
func (mc *MergedChan[T]) RemovePubs(names ...string) {
	if !mc.isPub {
		return
	}
	for _, name := range names {
		mc.channels.Delete(name)
	}
}

// AddSubs adds more sub chans to the MergedChan, skipping if the chan is
// already a part of it. A channel will also not be added if it's client is not
// the same as the MergedChan's client, if it's not a sub channel, or if it is
// closed. Does nothing if the MergedChan is not sub. If possible, it is more
// efficient to add more channels at a time than having multiple small calls
// (uses utils.SelectN under the hood), with the most efficient being adding
// channels in multiples of the highest utils.Select* function.
func (mc *MergedChan[T]) AddSubs(subChans ...*Channel[T]) {
	if mc.isPub || mc.IsStopped() {
		return
	}
	chans := make([]<-chan ChannelMsg[T], 0, len(subChans))
	for _, ch := range subChans {
		if ch.client == mc.client && !ch.isPub && !ch.IsClosed() {
			if _, loaded := mc.channels.LoadOrStore(ch.name, ch); !loaded {
				chans = append(chans, ch.Chan())
			}
		}
	}
	if len(chans) == 0 {
		return
	}
	mc.stopChansMtx.Lock()
	if mc.IsStopped() {
		return
	}
	mc.stopChans = append(mc.stopChans, utils.SelectN(mc.out, chans...))
	mc.stopChansMtx.Unlock()
}

// IsStopped returns whether the merged chan is stopped or not.
func (mc *MergedChan[T]) IsStopped() bool {
	return mc.isStopped.Load()
}

// Stop stops the merged chan and closes the output chan. Calling this has no
// effect if the merged chan is pub.
func (mc *MergedChan[T]) Stop() {
	if mc.isPub {
		return
	}
	if mc.isStopped.Swap(true) {
		return
	}
	mc.stopChansMtx.Lock()
	for _, ch := range mc.stopChans {
		close(ch)
	}
	mc.stopChansMtx.Unlock()
	close(mc.stopChan)
}

// ChannelMsg represents a message sent on a channel.
type ChannelMsg[T any] struct {
	channel *Channel[T]
	val     T
}

// Channel returns the channel that the message was sent on.
func (cm *ChannelMsg[T]) Channel() *Channel[T] {
	return cm.channel
}

// ChannelName returns the name of the channel that the message was sent on.
func (cm *ChannelMsg[T]) ChannelName() string {
	return cm.channel.name
}

// Value returns the value of the message.
func (cm *ChannelMsg[T]) Value() T {
	return cm.val
}

// Channel represents a pub or sub channel used to send/receive messages.
type Channel[T any] struct {
	client  *Client[T]
	network *Network[T]

	name     string
	isPub    bool
	isClosed *atomic.Bool

	subChan  chan ChannelMsg[T]
	subChans *utils.SyncMap[*Client[T], chan ChannelMsg[T]]

	multiplePubs bool
	pubCount     *atomic.Int32

	// The exclusive write lock is held whenever it's time to close the Channel.
	// It is used to close all the subChans chans so nothing is sent over a
	// closed chan. Methods sending over a chan should check if isClosed is set
	// while they hold the read lock.
	chanMtx *sync.RWMutex
}

// Client returns the client associated to the channel.
func (ch *Channel[T]) Client() *Client[T] {
	return ch.client
}

func (ch *Channel[T]) Network() *Network[T] {
	return ch.network
}

// Name returns the name of the channel.
func (ch *Channel[T]) Name() string {
	return ch.name
}

/* TODO: MULTIPUB
// MultiplePubs returns true if the channel can have multiple publishers.
func (ch *Channel[T]) MultiplePubs() bool {
  return ch.multiplePubs
}
*/

// IsPub returns true if the channel is a pub channel.
func (ch *Channel[T]) IsPub() bool {
	return ch.isPub
}

// Pub publishes to the channel, returning false if the channel is closed or it
// is not a pub channel.
func (ch *Channel[T]) Pub(t T) bool {
	if !ch.isPub {
		return false
	}
	ch.chanMtx.RLock()
	defer ch.chanMtx.RUnlock()
	if ch.isClosed.Load() {
		return false
	}
	msg := ChannelMsg[T]{channel: ch, val: t}
	ch.subChans.Range(func(_ *Client[T], c chan ChannelMsg[T]) bool {
		// NOTE: See close(c) in Channel.unsub
		select {
		case c <- msg:
		default:
		}
		return true
	})
	return true
}

// Unsub unsubscribes the client from the channel.
func (ch *Channel[T]) Unsub() {
	ch.unsub(true)
}

// If true, the chanMtx read lock will be held. Passing false is useful
// (required) for scenarios like avoiding a deadlock when this ends up getting
// called from Channel.Close since the write lock is held during that call.
func (ch *Channel[T]) unsub(lock bool) {
	if ch.isPub {
		return
	}
	c, loaded := ch.subChans.LoadAndDelete(ch.client)
	if !loaded {
		return
	}
	ch.client.subs.Delete(ch.name)
	if lock {
		ch.chanMtx.RLock()
		defer ch.chanMtx.RUnlock()
	}
	/*
		// Only close the chan if the channel is still open (chan isn't/will be
		// closed) to avoid double close (done in Channel.Close)
		if !ch.isClosed.Load() {
		}
	*/
	// NOTE: There is no locking to sync the channel close here and the write in
	// Channel.Pub because I am betting based on the internals of sync.Map, the
	// assumption I have of the speed of select, and my assumption on the time it
	// will take to reach this point in this function from the LoadAndDelete
	// above that there won't be an issue with sending on a closed channel.
	// I may die on this hill :|, but hopefully not :)
	close(c)
}

// Close closes the channel if this instance is pub, returning true if this
// call closed the channel and false if the channel was already closed, if this
// this instance isn't pub, or if it's multipub and there are more pubs.
func (ch *Channel[T]) Close() bool {
	// Don't execute if the channel still has more pubs or is already closed
	// (hence the checked swap).
	if !ch.isPub || ch.pubCount.Add(-1) > 0 {
		return false
	} else if ch.isClosed.Swap(true) {
		return false
	}

	ch.network.channels.Delete(ch.name)
	ch.client.pubs.Delete(ch.name)
	ch.chanMtx.Lock()
	// Go through and close all the chans while holding the write lock
	ch.subChans.Range(func(client *Client[T], c chan ChannelMsg[T]) bool {
		client.unsub(ch.name, false)
		return true
	})
	ch.chanMtx.Unlock()
	return true
}

// IsClosed returns true if the channel is closed.
func (ch *Channel[T]) IsClosed() bool {
	return ch.isClosed.Load()
}

// Chan returns the chan the data is received on.
// This returned chan will be closed when the channel is closed, thus,
// receivers should receive from the chan in a way that recognized closes.
func (ch *Channel[T]) Chan() <-chan ChannelMsg[T] {
	return ch.subChan
}

/* TODO: MULTIPUB
// newPub attemps to add a new pub to the channel. Returns the channel and true
// if the client was not
func (ch *Channel[T]) newPub(client *Client[T]) (*Channel[T], bool) {
  if !ch.multiplePubs || ch.IsClosed() {
    return nil, false
  } else if ch.client == client {
    return ch, false
  }
  ch.pubCount.Add(1)
  retCh := ch.clone()
  retCh.client, retCh.isPub = client, true
  return retCh, true
}
*/

// Will return a new channel instance with an existing subChan if one already
// exists (the client is already subscribed). The function will return true if
// the client wasn't previously subbed, false otherwise.
func (ch *Channel[T]) newSub(client *Client[T], chanLen uint32) (*Channel[T], bool) {
	ch.chanMtx.RLock()
	defer ch.chanMtx.RUnlock()
	if ch.isClosed.Load() {
		return nil, false
	}
	subChan, loaded := ch.subChans.LoadOrStore(client, make(chan ChannelMsg[T], chanLen))
	retCh := ch.clone()
	retCh.client, retCh.isPub, retCh.subChan = client, false, subChan
	return retCh, !loaded
}

// Returns nil if the client isn't subbed to the channel, otherwise, returns a
// new channel instance with the client and its subChan. Basically the same as
// Channel.newSub but doesn't make a new sub if the client isn't subbed.
func (ch *Channel[T]) cloneWithClient(client *Client[T]) *Channel[T] {
	ch.chanMtx.RLock()
	defer ch.chanMtx.RUnlock()
	subChan, loaded := ch.subChans.Load(client)
	if !loaded {
		return nil
	}
	retCh := ch.clone()
	retCh.client, retCh.isPub, retCh.subChan = client, false, subChan
	return retCh
}

// Clones the channel without the client, isPub, and subChan being set.
func (ch *Channel[T]) clone() *Channel[T] {
	return &Channel[T]{
		network:  ch.network,
		name:     ch.name,
		isClosed: ch.isClosed,
		subChans: ch.subChans,
		chanMtx:  ch.chanMtx,
	}
}
