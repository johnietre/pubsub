package client

import (
  "errors"
  "net"
  "strings"
  "sync/atomic"

  "pubsub/go/internal/common"
)

var (
  ErrChanExist = errors.New("channel already exists")
  ErrChanNotExist = errors.New("channel does not exist")
  ErrServerError = errors.New("server error")
  ErrChanClosed = errors.New("channel deleted")
  ErrChanIsPub = errors.New("channel is a publisher")
  ErrChanNotPub = errors.New("channel is not a publisher")
)

type Client struct {
  conn net.Conn
  subs *common.SyncMap[string, *Channel]
  pubs *common.SyncMap[string, *Channel]

  msgQueueLen uint
  discardOnFull bool
}

func NewClient(addr string, queueLen uint, discard bool) (*Client, error) {
  conn, err := net.Dial("tcp", addr)
  if err != nil {
    return nil, err
  }
  c := &Client{
    conn: conn,
    subs: common.NewSyncMap[string, *Channel](),
    pubs: common.NewSyncMap[string, *Channel](),
    msgQueueLen: queueLen,
    discardOnFull: discard,
  }
  go c.recv()
  return c, nil
}

func NewClientFromConn(conn net.Conn, queueLen uint, discard bool) *Client {
  c := &Client{
    conn: conn,
    subs: common.NewSyncMap[string, *Channel](),
    pubs: common.NewSyncMap[string, *Channel](),
    msgQueueLen: queueLen,
    discardOnFull: discard,
  }
  go c.recv()
  return c
}

// Sub sends a subscribe message to the server
// It will return an error
func (c *Client) Sub(names ...string) error {
  if err := c.sendMsg(common.MsgTypeSub, strings.Join(names, "\n")); err != nil {
    return err
  }
  for _, name := range names {
    c.subs.LoadOrStore(name, c.newChannel(name, c.msgQueueLen, false))
  }
  return nil
}

func (c *Client) Unsub(names ...string) error {
  if err := c.sendMsg(common.MsgTypeUnsub, strings.Join(names, "\n")); err != nil {
    return err
  }
  for _, name := range names {
    if channel, loaded := c.subs.LoadAndDelete(name); loaded {
      channel.err.Store(ErrChanClosed)
    }
  }
  return nil
}

func (c *Client) Pub(msg string, names ...string) error {
  return c.sendMsg(common.MsgTypePub, strings.Join(names, "\n")+"\r"+msg)
}

func (c *Client) NewChan(names ...string) error {
  if err := c.sendMsg(common.MsgTypeNewChan, strings.Join(names, "\n")); err != nil {
    return err
  }
  for _, name := range names {
    c.pubs.LoadOrStore(name, c.newChannel(name, c.msgQueueLen, true))
  }
  return nil
}

func (c *Client) DelChan(names ...string) error {
  if err := c.sendMsg(common.MsgTypeDelChan, strings.Join(names, "\n")); err != nil {
    return err
  }
  for _, name := range names {
    if channel, loaded := c.pubs.LoadAndDelete(name); loaded {
      channel.err.Store(ErrChanClosed)
    }
  }
  return nil
}

func (c *Client) GetSub(name string) (*Channel, bool) {
  return c.subs.Load(name)
}

func (c *Client) GetPub(name string) (*Channel, bool) {
  return c.pubs.Load(name)
}

// GetChan gets a channel for the given name
// If there are a sub and pub channel with the same name, the sub is returned
func (c *Client) GetChan(name string) (*Channel, bool) {
  if channel, loaded := c.subs.Load(name); loaded {
    return channel, true
  }
  return c.pubs.Load(name)
}

func (c *Client) GetSubs(names ...string) []*Channel {
  var chans []*Channel
  for _, name := range names {
    if ch, loaded := c.subs.Load(name); loaded {
      chans = append(chans, ch)
    }
  }
  return chans
}

func (c *Client) GetPubs(names ...string) []*Channel {
  var chans []*Channel
  for _, name := range names {
    if ch, loaded := c.pubs.Load(name); loaded {
      chans = append(chans, ch)
    }
  }
  return chans
}

// GetChans returns all channels with the names given, even if there are sub
// and pub versions (both are returned)
func (c *Client) GetChans(names ...string) []*Channel {
  var chans []*Channel
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
  for {
    c.subs.Range(func(name string, ch *Channel) bool {
      msg, got, err = ch.Recv(false)
      if got || err != nil {
        chanName = name
        return false
      }
      return true
    })
    if chanName != "" {
      return
    }
  }
}

// Closes the client
func (c *Client) Close() error {
  c.subs.Range(func(name string, ch *Channel) bool {
    c.subs.Delete(name)
    ch.err.Store(ErrChanClosed)
    return true
  })
  c.pubs.Range(func(name string, ch *Channel) bool {
    c.pubs.Delete(name)
    ch.err.Store(ErrChanClosed)
    return true
  })
  return c.conn.Close()
}

func (c *Client) sendMsg(mt common.MsgType, msg string) error {
  _, err := c.conn.Write(common.EncodeMsg(mt, msg))
  return err
}

func (c *Client) recv() {
  var initial [5]byte
  for {
    n, err := c.conn.Read(initial[:])
    // TODO: Propogate error
    if err != nil {
      c.subs.Range(func(_ string, ch *Channel) bool {
        ch.err.Store(err)
        return true
      })
      c.pubs.Range(func(_ string, ch *Channel) bool {
        ch.err.Store(err)
        return true
      })
      return
    }
    if n != 5 {
      continue
    }
    l := common.Get4(initial[1:])
    buf := make([]byte, l)
    n, err = c.conn.Read(buf)
    // TODO: Propogate error
    if err != nil {
      c.subs.Range(func(_ string, ch *Channel) bool {
        ch.err.Store(err)
        return true
      })
      c.pubs.Range(func(_ string, ch *Channel) bool {
        ch.err.Store(err)
        return true
      })
      return
    }
    // TODO: Possibly handle mismatch len here
    mt, msg, err := common.DecodeMsg(append(initial[:], buf...))
    if err != nil {
      continue
    }
    switch mt {
    case common.MsgTypePub:
      parts := strings.SplitN(msg, "\r", 2)
      if len(parts) != 2 {
        continue
      }
      // NOTE: Possibly do something if channel isn't loaded
      if channel, loaded := c.subs.Load(parts[0]); loaded {
        select {
        case channel.ch <- parts[1]: // Send successfully
        default:
          // Check if it needs to be discarded
          if c.discardOnFull {
            select {
            case <-channel.ch: // Possibility it's already been read from
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
      }
    case common.MsgTypeSub:
      for _, name := range strings.Split(msg, "\n") {
        if channel, loaded := c.subs.LoadAndDelete(name); loaded {
          channel.err.Store(ErrChanNotExist)
        }
      }
    case common.MsgTypeNewChan:
      for _, name := range strings.Split(msg, "\n") {
        if channel, loaded := c.pubs.LoadAndDelete(name); loaded {
          channel.err.Store(ErrChanExist)
        }
      }
    case common.MsgTypeDelChan:
      if channel, loaded := c.subs.LoadAndDelete(msg); loaded {
        channel.err.Store(ErrChanClosed)
      }
    }
  }
}

func (c *Client) newChannel(name string, l uint, pub bool) *Channel {
  return &Channel{client: c, name: name, ch: make(chan string, l), isPub: pub}
}

type Channel struct {
  client *Client
  name string
  // TODO: possibly close chan on delete
  ch chan string
  isPub bool
  err atomic.Value
}

func (ch *Channel) Client() *Client {
  return ch.client
}

func (ch *Channel) Name() string {
  return ch.name
}

func (ch *Channel) IsPub() bool {
  return ch.isPub
}

func (ch *Channel) Err() error {
  err := ch.err.Load()
  if err != nil {
    return err.(error)
  }
  return nil
}

// Recv receives a message, blocking the thread if true
// If a message is received, msg, true, nil is returned
// If false, it returns "", false, nil if there's no message and no error
// If there's an error, "", false, err is returned
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
        return "", false, err.(error)
      }
    }
    return "", false, nil
  }
  for {
    select {
    case msg := <-ch.ch:
      return msg, true, nil
    default:
      err := ch.err.Load()
      if err != nil {
        return "", false, err.(error)
      }
    }
  }
}

func (ch *Channel) Pub(msg string) error {
  if !ch.isPub {
    return ErrChanNotPub
  }
  if err := ch.err.Load(); err != nil {
    return err.(error)
  }
  return ch.client.sendMsg(common.MsgTypePub, ch.name+"\r"+msg)
}

// Close unsubs or deletes a channel, depending on if it's a publisher or not
func (ch *Channel) Close() {
  if ch.err.Load() != nil {
    return
  }
  ch.err.Store(ErrChanClosed)
  if !ch.isPub {
    ch.client.sendMsg(common.MsgTypeUnsub, ch.name)
    ch.client.subs.Delete(ch.name)
    return
  }
  ch.client.sendMsg(common.MsgTypeDelChan, ch.name)
  ch.client.pubs.Delete(ch.name)
}
