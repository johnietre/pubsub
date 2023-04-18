package npmc

import (
	"sync"
	"sync/atomic"

	"github.com/johnietre/pubsub/utils"
)

// Netowrk is a network of channels that clients can pub/sub to.
type Network[T any] struct {
	channels *utils.SyncMap[string, *Channel[T]]
}

// NetNetwork creates a new network.
func NewNetwork[T any]() *Network[T] {
	return &Network[T]{channels: utils.NewSyncMap[string, *Channel[T]]()}
}

// NewClient creates a new client on the network.
func (n *Network[T]) NewClient() *Client[T] {
	return &Client[T]{
		network: n,
		pubs:    utils.NewSyncMap[string, *Channel[T]](),
		subs:    utils.NewSyncMap[string, *Channel[T]](),
	}
}

// Client represents a client of on network that can publish (create) and
// subscribe to channels.
type Client[T any] struct {
	network  *Network[T]
	pubs     *utils.SyncMap[string, *Channel[T]]
	subs     *utils.SyncMap[string, *Channel[T]]
	isClosed atomic.Bool
	// This is used to help syncronize when the channel is closed
	closeMtx sync.RWMutex
}

// Network returns the network the client is associated with.
func (c *Client[T]) Network() *Network[T] {
	return c.network
}

// NewPub creates a new pub channel if the channel doesn't already exist. If it
// does, it returns nil, false. Additionally, nil, false is returned if the
// channel is closed.
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
		chanMtx:  &sync.RWMutex{},
	}
	ch, loaded := c.network.channels.LoadOrStore(name, ch)
	if loaded {
		return nil, false
	}
	return ch, true
}

// ClosePub closes the pub channel with the given name, if it exists and the
// client is the publisher.
func (c *Client[T]) ClosePub(name string) {
	ch, loaded := c.pubs.Load(name)
	if !loaded {
		return
	}
	ch.Close()
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
// false is returned if the channel is closed.
func (c *Client[T]) NewSub(name string, chanLen uint32) (*Channel[T], bool) {
	c.closeMtx.RLock()
	defer c.closeMtx.RUnlock()
	if c.isClosed.Load() {
		return nil, false
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

// Unsub unsubscribes from a channel.
func (c *Client[T]) Unsub(name string) {
	ch, loaded := c.subs.Load(name)
	if !loaded {
		return
	}
	ch.Unsub()
}

// UnsubAll unsubscribes from all channels.
func (c *Client[T]) UnsubAll() {
	c.subs.Range(func(name string, _ *Channel[T]) bool {
		c.Unsub(name)
		return true
	})
}

// Close closes the client, unsubscribing (sub) and closing (pub) all channels.
func (c *Client[T]) Close() {
	// Avoid an unnecessary locking if the channel is already closed.
	if c.isClosed.Swap(true) {
		return
	}
	c.closeMtx.Lock()
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
		if ch, loaded := c.subs.Load(name); loaded {
			channels.Store(name, ch)
			chans = append(chans, ch.Chan())
		}
	}
	out := make(chan ChannelMsg[T], chanLen)
	stopChan := utils.SelectN(out, chans...)
	return &MergedChan[T]{
		channels: channels,
		isPub:    false,
		out:      out,
		stopChan: stopChan,
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
	c.subs.Range(func(name string, ch *Channel[T]) bool {
		channels.Store(name, ch)
		chans = append(chans, ch.Chan())
		return true
	})
	out := make(chan ChannelMsg[T], chanLen)
	stopChan := utils.SelectN(out, chans...)
	return &MergedChan[T]{
		channels: channels,
		isPub:    false,
		out:      out,
		stopChan: stopChan,
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
	channels  *utils.SyncMap[string, *Channel[T]]
	isPub     bool
	out       <-chan ChannelMsg[T]
	stopChan  chan<- utils.Unit
	isStopped atomic.Bool
	// No mutex is used since nothing is ever added to the struct fields; the
	// isStopped atomic can handle synchronization of closing stopChan.
}

// Pub publishes the given value to the channels if the merged chan is pub.
// Nothing is returned, even if all the channels are closed (nothing is done if
// this is the case).
func (mc *MergedChan[T]) Pub(t T) {
	mc.channels.Range(func(_ string, ch *Channel[T]) bool {
		ch.Pub(t)
		return true
	})
}

// RemovePubs removes the specified channels from the list, if they exist.
// Removing a channel does not close it. Nothing is done is the merged channel
// is not pub (see type documentation for further explanation).
func (mc *MergedChan[T]) RemovePubs(names ...string) {
	if !mc.isPub {
		return
	}
	for _, name := range names {
		mc.channels.Delete(name)
	}
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
	if ch.isPub {
		return
	}
	c, loaded := ch.subChans.LoadAndDelete(ch.client)
	if !loaded {
		return
	}
	ch.client.subs.Delete(ch.name)
	ch.chanMtx.RLock()
	defer ch.chanMtx.RUnlock()
	// Only close the chan if the channel is still open (chan isn't/will be
	// closed) to avoid double close
	if !ch.isClosed.Load() {
		close(c)
	}
}

// Close closes the channel if this instance is pub (IsPub() == true),
// returning true if this call closed the channel and false if the channel was
// already closed or if this instance isn't pub.
func (ch *Channel[T]) Close() bool {
	// Don't execute if the channel is already closed (hence the checked swap).
	if !ch.isPub || ch.isClosed.Swap(true) {
		return false
	}
	ch.client.pubs.Delete(ch.name)
	ch.network.channels.Delete(ch.name)
	ch.chanMtx.Lock()
	// Go through and close all the chans while holding the write lock
	ch.subChans.Range(func(client *Client[T], c chan ChannelMsg[T]) bool {
		ch.subChans.Delete(client)
		close(c)
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
	return &Channel[T]{
		client:   client,
		network:  client.network,
		name:     ch.name,
		isPub:    false,
		isClosed: ch.isClosed,
		subChan:  subChan,
		subChans: ch.subChans,
		chanMtx:  ch.chanMtx,
	}, !loaded
}
