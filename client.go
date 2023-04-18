package pubsub

import (
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnietre/pubsub/utils"
)

const timeoutDur = time.Second * 5

var (
	// ErrChanExist is the error used when trying to create an already existing
	// channel.
	ErrChanExist = errors.New("channel already exists")
	// ErrChanNotExist is the error used when trying to sub to an channel that
	// doesn't exist.
	ErrChanNotExist = errors.New("channel does not exist")
	// ErrServerError is the result of a server error
	ErrServerError = errors.New("server error")
	// ErrChanClosed is the error returned when trying to operate on a closed.
	// channel
	ErrChanClosed = errors.New("channel deleted")
	// ErrChanIsPub is returned when trying to Recv from a pub channel.
	ErrChanIsPub = errors.New("channel is a publisher")
	// ErrChanNotPub is returned when trying to Pub from a sub channel.
	ErrChanNotPub = errors.New("channel is not a publisher")
)

// Client is a client linked to a Server.
// TODO: Keep track of the number of channels
type Client struct {
	conn         net.Conn
	subs         *utils.SyncMap[string, *Channel]
	pubs         *utils.SyncMap[string, *Channel]
	allChanNames *utils.SyncSet[string]
	// Used to lock when updating names
	allChansMtx      sync.Mutex
	lastChansRefresh int64

	msgQueueLen   uint32
	discardOnFull bool

	timeoutChans *utils.SyncSet[chan utils.Unit]
}

// NewClient creates a new client connected to the server on the given addr.
// The queueLen is the length of the message backlog when messages aren't
// actively being Recv'ed. discard specifies whether messages should be
// discarded when the message backlog is full. If not, the entire client
// process will block and no new messages will be received/processed until Recv
// is called again.
func NewClient(addr string, queueLen uint32, discard bool) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewClientFromConn(conn, queueLen, discard), nil
}

// NewClientFromConn creates a new client from the given net.Conn
func NewClientFromConn(conn net.Conn, queueLen uint32, discard bool) *Client {
	c := &Client{
		conn:          conn,
		subs:          utils.NewSyncMap[string, *Channel](),
		pubs:          utils.NewSyncMap[string, *Channel](),
		allChanNames:  utils.NewSyncSet[string](),
		msgQueueLen:   queueLen,
		discardOnFull: discard,
		timeoutChans:  utils.NewSyncSet[chan utils.Unit](),
	}
	go c.recv()
	go c.runTimeout()
	return c
}

// Sub sends a subscribe message to the server.
// It will return an error only if sending the message itself results in error.
// It will also add the channels passed to the internally held channel names of
// the client, even if the channels don't exist.
func (c *Client) Sub(names ...string) error {
	if err := c.sendMsg(utils.MsgTypeSub, strings.Join(names, "\n")); err != nil {
		return err
	}
	for _, name := range names {
		c.subs.LoadOrStore(name, c.newChannel(name, c.msgQueueLen, false))
	}
	return nil
}

// Unsub sends an unsubscribe message to the server.
// It will return an error only if sending the message itself results in error.
// It removes all passed channels from the client's internal list of channels.
func (c *Client) Unsub(names ...string) error {
	if err := c.sendMsg(utils.MsgTypeUnsub, strings.Join(names, "\n")); err != nil {
		return err
	}
	for _, name := range names {
		if channel, loaded := c.subs.LoadAndDelete(name); loaded {
			channel.err.Store(ErrChanClosed)
		}
	}
	return nil
}

// Pub sends a publish message to the server for the given channels.
// It will return an error only if sending the message itself results in error.
// If the client doesn't own those channels, nothing will be published.
func (c *Client) Pub(msg string, names ...string) error {
	return c.sendMsg(utils.MsgTypePub, strings.Join(names, "\n")+"\r"+msg)
}

// PubBytes takes bytes and does the same thing as Pub.
func (c *Client) PubBytes(msg []byte, names ...string) error {
	return c.sendMsg(utils.MsgTypePub, strings.Join(names, "\n")+"\r"+string(msg))
}

// NewChan creates a new channel for publishing on for the client.
// It will return an error only if sending the message itself results in error.
// It will add the names to the client's list of channels, even if the channel
// already exists
func (c *Client) NewChan(names ...string) error {
	if err := c.sendMsg(utils.MsgTypeNewChan, strings.Join(names, "\n")); err != nil {
		return err
	}
	for _, name := range names {
		c.pubs.LoadOrStore(name, c.newChannel(name, 0, true))
	}
	return nil
}

// NewMultiChan creates a new channel that can have multiple publishers
func (c *Client) NewMultiChan(names ...string) error {
	if err := c.sendMsg(utils.MsgTypeNewMultiChan, strings.Join(names, "\n")); err != nil {
		return err
	}
	for _, name := range names {
		c.pubs.LoadOrStore(name, c.newChannel(name, 0, true))
	}
	return nil
}

// DelChan deletes a channel the client previously created
// It will return an error only if sending the message itself results in error.
// It will delete the names from the client's list of channels.
// If the client doesn't own a channel, nothing happens.
func (c *Client) DelChan(names ...string) error {
	if err := c.sendMsg(utils.MsgTypeDelChan, strings.Join(names, "\n")); err != nil {
		return err
	}
	for _, name := range names {
		if channel, loaded := c.pubs.LoadAndDelete(name); loaded {
			channel.err.Store(ErrChanClosed)
		}
	}
	return nil
}

// GetSub gets the sub channel from the client's internal list if it exists.
func (c *Client) GetSub(name string) (*Channel, bool) {
	return c.subs.Load(name)
}

// GetPub gets the pub channel from the client's internal list if it exists.
func (c *Client) GetPub(name string) (*Channel, bool) {
	return c.pubs.Load(name)
}

// GetChan gets a channel for the given name.
// If there are a sub and pub channel with the same name, the sub is returned.
func (c *Client) GetChan(name string) (*Channel, bool) {
	if channel, loaded := c.subs.Load(name); loaded {
		return channel, true
	}
	return c.pubs.Load(name)
}

// GetSubs returns a list of sub channels for the given names past, excluding
// names that don't exist.
func (c *Client) GetSubs(names ...string) []*Channel {
	chans := make([]*Channel, 0, len(names))
	if len(names) == 0 {
		c.subs.Range(func(_ string, ch *Channel) bool {
			chans = append(chans, ch)
			return true
		})
		return chans
	}
	for _, name := range names {
		if ch, loaded := c.subs.Load(name); loaded {
			chans = append(chans, ch)
		}
	}
	return chans
}

// GetPubs returns a list of pub channels for the given names past, excluding
// names that don't exist.
func (c *Client) GetPubs(names ...string) []*Channel {
	var chans []*Channel
	if len(names) == 0 {
		c.pubs.Range(func(_ string, ch *Channel) bool {
			chans = append(chans, ch)
			return true
		})
		return chans
	}
	for _, name := range names {
		if ch, loaded := c.pubs.Load(name); loaded {
			chans = append(chans, ch)
		}
	}
	return chans
}

// GetChans returns all channels with the names given, even if there are sub
// and pub versions (both are returned).
func (c *Client) GetChans(names ...string) []*Channel {
	var chans []*Channel
	if len(names) == 0 {
		c.subs.Range(func(_ string, ch *Channel) bool {
			chans = append(chans, ch)
			return true
		})
		c.pubs.Range(func(_ string, ch *Channel) bool {
			chans = append(chans, ch)
			return true
		})
		return chans
	}
	for _, name := range names {
		if ch, loaded := c.subs.Load(name); loaded {
			chans = append(chans, ch)
		}
		if ch, loaded := c.pubs.Load(name); loaded {
			chans = append(chans, ch)
		}
	}
	return chans
}

// Recv reads a message from the server, returning the channel name, message,
// if a message was received, and any errors
// Same as channel.Recv() but returns the channel name too
func (c *Client) Recv(blocking bool) (chanName string, msg string, got bool, err error) {
	if !blocking {
		c.subs.Range(func(name string, ch *Channel) bool {
			msg, got, err = ch.Recv(false)
			if got || err != nil {
				return false
			}
			return true
		})
		return
	}
	timeoutChan := c.getTimeoutChan()
	refTChan := reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(timeoutChan),
	}
	defer c.retTimeoutChan(timeoutChan)
	for {
		cases := []reflect.SelectCase{refTChan}
		var chans []*Channel
		c.subs.Range(func(name string, ch *Channel) bool {
			chans = append(chans, ch)
			cases = append(
				cases,
				reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ch.ch),
				},
			)
			return true
		})
		i, recv, ok := reflect.Select(cases)
		i--
		if i == -1 {
			// The msg received came from the timeout
			continue
		}
		// A message was received
		if ok {
			return chans[i].name, recv.String(), true, nil
		}
		// There was a channel error
		return chans[i].name, "", true, chans[i].Err()
		// If this point was reached, the operation timed out
	}
}

// RecvBytes does the same thing as Recv but returns the message as bytes
func (c *Client) RecvBytes(blocking bool) (chanName string, msg []byte, got bool, err error) {
	var smsg string
	chanName, smsg, got, err = c.Recv(blocking)
	msg = []byte(smsg)
	return
}

// Closes the client, deleting the channels and closing the underlying client.
func (c *Client) Close() error {
	c.subs.Range(func(name string, ch *Channel) bool {
		ch.closeChan(false)
		return true
	})
	c.pubs.Range(func(name string, ch *Channel) bool {
		ch.closeChan(false)
		return true
	})
	return c.conn.Close()
}

// RefreshAllChanNames refreshes the client's list of all channel names from
// the server. It returns the time the message was sent or an error if one was
// encountered while sending (won't return 0 time on error).
// The time is gotten before sending because, in some cases during testing,
// somehow, the time gotten after sending the message was later than the time
// gotten after receiving the updated names from the server and updating the
// client's internal list.
func (c *Client) RefreshAllChanNames() (int64, error) {
	t := time.Now().UnixNano()
	err := c.sendMsg(utils.MsgTypeChanNames, "")
	return t, err
}

// WairForChansRefresh waits for a names refresh from the server.
// Takes a timestamp t (Unix timestamp with nanosecond precision) returned from
// RefreshAllChanNames to compare to when the last refresh from the server
// occurred.
// Also takes a timeout as a time in seconds. Set timeout as -1 for no timeout
// Returns true if the last refresh time was after the given timestamp t.
func (c *Client) WaitForChansRefresh(t int64, timeout time.Duration) bool {
	var end time.Time
	if timeout > 0 {
		end = time.Now().Add(timeout * time.Second)
	} else {
		end = time.Now().AddDate(100, 0, 0)
	}
	for time.Now().Before(end) {
		if atomic.LoadInt64(&c.lastChansRefresh) > t {
			return true
		}
	}
	return atomic.LoadInt64(&c.lastChansRefresh) > t
}

func (c *Client) GetAllChanNames() *utils.SyncSet[string] {
	return c.allChanNames
}

// ClearServerChans deletes the list of chan names held by the client and
// creates a new one. All prev distributed refs will no longer be updated.
func (c *Client) ClearServerChans() {
	c.allChansMtx.Lock()
	c.allChanNames = utils.NewSyncSet[string]()
	c.allChansMtx.Unlock()
}

func (c *Client) sendMsg(mt utils.MsgType, msg string) error {
	_, err := c.conn.Write(utils.EncodeMsg(mt, msg))
	return err
}

func (c *Client) recv() {
	var initial [5]byte
	for {
		n, err := c.conn.Read(initial[:])
		// TODO: Propogate error
		if err != nil {
			c.subs.Range(func(_ string, ch *Channel) bool {
				ch.closeChan(false)
				return true
			})
			c.pubs.Range(func(_ string, ch *Channel) bool {
				ch.closeChan(false)
				return true
			})
			return
		}
		if n != 5 {
			continue
		}
		l := utils.Get4(initial[1:])
		buf := make([]byte, l)
		n, err = io.ReadFull(c.conn, buf)
		// TODO: Propogate error
		if err != nil {
			c.subs.Range(func(_ string, ch *Channel) bool {
				ch.closeChan(false)
				return true
			})
			c.pubs.Range(func(_ string, ch *Channel) bool {
				ch.closeChan(false)
				return true
			})
			return
		}
		// TODO: Possibly handle mismatch len here
		mt, msg, err := utils.DecodeMsg(append(initial[:], buf...))
		if err != nil {
			continue
		}
		switch mt {
		case utils.MsgTypePub:
			parts := strings.SplitN(msg, "\r", 2)
			if len(parts) != 2 {
				continue
			}
			// NOTE: Possibly do something if channel isn't loaded
			if channel, loaded := c.subs.Load(parts[0]); loaded {
				if channel.Err() != nil {
					continue
				}
				func() {
					defer func() {
						// Just in case the channel is closed after the check earlier
						recover()
					}()
					select {
					case channel.ch <- parts[1]: // Send successfully
					default:
						// Check if it needs to be discarded
						if c.discardOnFull {
							select {
							case _, ok := <-channel.ch: // Possibility it's already been read from
								// Channel has been closed
								if !ok {
									return
								}
							default:
							}
							// TODO: Should never be full after this so using select should be
							// unnecessary
							select {
							case channel.ch <- parts[1]:
							default:
							}
						}
					}
				}()
			}
		case utils.MsgTypeSub:
			for _, name := range strings.Split(msg, "\n") {
				if channel, loaded := c.subs.Load(name); loaded {
					channel.closeChanWithErr(false, ErrChanNotExist)
				}
			}
		case utils.MsgTypeNewChan, utils.MsgTypeNewMultiChan:
			for _, name := range strings.Split(msg, "\n") {
				if channel, loaded := c.pubs.LoadAndDelete(name); loaded {
					channel.closeChanWithErr(false, ErrChanExist)
				}
			}
		case utils.MsgTypeDelChan:
			if channel, loaded := c.subs.Load(msg); loaded {
				channel.closeChan(false)
			}
		case utils.MsgTypeChanNames:
			// Needs to be locked in case there's a call to clear the names
			c.allChansMtx.Lock()
			set := utils.NewSet[string]()
			for _, name := range strings.Split(msg, "\n") {
				set.Insert(name)
			}
			set.Range(func(name string) bool {
				c.allChanNames.Insert(name)
				return true
			})
			c.allChanNames.Range(func(name string) bool {
				if !set.Contains(name) {
					c.allChanNames.Remove(name)
				}
				return true
			})
			atomic.StoreInt64(&c.lastChansRefresh, time.Now().UnixNano())
			c.allChansMtx.Unlock()
		default:
		}
	}
}

func (c *Client) newChannel(name string, l uint32, pub bool) *Channel {
	return &Channel{client: c, name: name, ch: make(chan string, l), isPub: pub}
}

func (c *Client) runTimeout() {
	for {
		time.Sleep(timeoutDur)
		c.timeoutChans.Range(func(ch chan utils.Unit) bool {
			select {
			case ch <- utils.Unit{}:
			default:
			}
			return true
		})
	}
}

func (c *Client) getTimeoutChan() chan utils.Unit {
	ch := make(chan utils.Unit)
	c.timeoutChans.Insert(ch)
	return ch
}

func (c *Client) retTimeoutChan(ch chan utils.Unit) {
	c.timeoutChans.Remove(ch)
}

// Channel represents a channel.
type Channel struct {
	client *Client
	name   string
	ch     chan string
	isPub  bool
	err    atomic.Value
}

// Client gets the client the channel is associated with.
func (ch *Channel) Client() *Client {
	return ch.client
}

// Name returns the name of the channel.
func (ch *Channel) Name() string {
	return ch.name
}

// IsPub returns whether the channel can be published on or not.
func (ch *Channel) IsPub() bool {
	return ch.isPub
}

// Err returns whether there is an error on channel operations.
// If so, no channel operations will work.
func (ch *Channel) Err() error {
	err := ch.err.Load()
	if err != nil {
		return err.(error)
	}
	return nil
}

// Recv receives a message, blocking the thread if blocking is true.
// If a message is received, msg, true, nil is returned.
// If false, it returns "", false, nil if there's no message and no error.
// If there's an error, "", false, err is returned.
func (ch *Channel) Recv(blocking bool) (string, bool, error) {
	if ch.isPub {
		return "", false, ErrChanIsPub
	}
	if !blocking {
		select {
		case msg := <-ch.ch:
			return msg, true, nil
		default:
			err := ch.err.Load()
			if err != nil {
				ch.client.subs.Delete(ch.name)
				return "", false, err.(error)
			}
		}
		return "", false, nil
	}
	select {
	case msg, ok := <-ch.ch:
		if !ok {
			err := ch.err.Load()
			if err != nil {
				ch.client.subs.Delete(ch.name)
				return "", false, err.(error)
			}
		}
		return msg, true, nil
	}
}

// RecvBytes does the same the as Recv but returns the message as bytes
func (ch *Channel) RecvBytes(blocking bool) ([]byte, bool, error) {
	msg, ok, err := c.Recv(blocking)
	return []byte(msg), ok, err
}

// Pub publishes the given message to the channel, returning an error if the
// channel cannot be published to, if the channel is no longer operable, or if
// there was an error sending the actual mesage to the server.
func (ch *Channel) Pub(msg string) error {
	if !ch.isPub {
		return ErrChanNotPub
	}
	if err := ch.err.Load(); err != nil {
		return err.(error)
	}
	return ch.client.sendMsg(utils.MsgTypePub, ch.name+"\r"+msg)
}

// PubBytes takse a bytes message and does the same thing as Pub
func (ch *Channel) PubBytes(msg []byte) error {
	if !ch.isPub {
		return ErrChanNotPub
	}
	if err := ch.err.Load(); err != nil {
		return err.(error)
	}
	return ch.client.sendMsg(utils.MsgTypePub, ch.name+"\r"+string(msg))
}

// Close unsubs or deletes a channel, depending on if it's a publisher or not.
// It also removes the channel from the client's list.
func (ch *Channel) Close() {
	ch.closeChan(true)
}

func (ch *Channel) closeChan(sendMsg bool) {
	ch.closeChanWithErr(sendMsg, ErrChanClosed)
}

func (ch *Channel) closeChanWithErr(sendMsg bool, err error) {
	if !ch.err.CompareAndSwap(nil, err) {
		return
	}
	close(ch.ch)
	if !sendMsg {
		return
	}
	if !ch.isPub {
		ch.client.sendMsg(utils.MsgTypeUnsub, ch.name)
		return
	}
	ch.client.sendMsg(utils.MsgTypeDelChan, ch.name)
}
