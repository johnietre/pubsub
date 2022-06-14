package server

import (
	"log"
	"net"
	"strings"

	"pubsub/go/internal/common"
  clientpkg "pubsub/go/client"
)

const (
	bufSize = 1024
)

type Server struct {
  ln net.Listener
  channels *common.SyncMap[string, *Channel]
  errorLog *log.Logger
}

func NewServer(addr string) (*Server, error) {
  ln, err := net.Listen("tcp", addr)
  if err != nil {
    return nil, err
  }
  return &Server{
    ln: ln,
    channels: common.NewSyncMap[string, *Channel](),
    errorLog: log.Default(),
  }, nil
}

func NewLocalServer() *Server {
  return &Server{
    ln: nil,
    channels: common.NewSyncMap[string, *Channel](),
    errorLog: log.Default(),
  }
}

func (s *Server) Run() error {
  if s.ln == nil {
    return nil
  }
  for {
    c, err := s.ln.Accept()
    if err != nil {
      return err
    }
    go s.handle(c)
  }
}

func (s *Server) Shutdown() error {
  if s.ln == nil {
    return nil
  }
  return s.ln.Close()
}

func (s *Server) SetErrorLog(logger *log.Logger) {
  s.errorLog = logger
}

func (s *Server) GetErrorLog() *log.Logger {
  return s.errorLog
}

func (s *Server) NewClient(queueLen uint, discard bool) *clientpkg.Client {
  clientConn, serverConn := net.Pipe()
  go s.handle(serverConn)
  return clientpkg.NewClientFromConn(clientConn, queueLen, discard)
}

func (s *Server) handle(c net.Conn) {
	client := s.newClient(c)
	defer c.Close()
	defer func() {
		client.unsub("")
		client.delChan("")
	}()
	for {
    initial := make([]byte, 5)
    n, err := c.Read(initial[:])
		if err != nil {
			return
		}
		if n != 5 {
			client.sendMsg(common.MsgTypeErr, common.ErrBadMsg.Error())
			continue
		}
    l := common.Get4(initial[1:])
    buf := make([]byte, l)
    n, err = c.Read(buf)
		if err != nil {
			return
		}
    mt, msg, err := common.DecodeMsg(append(initial, buf[:n]...))
		if err != nil {
			client.sendMsg(common.MsgTypeErr, err.Error())
			continue
		}
		switch mt {
		case common.MsgTypePub:
			client.publish(msg)
		case common.MsgTypeSub:
			client.sub(msg)
		case common.MsgTypeUnsub:
			client.unsub(msg)
		case common.MsgTypeNewChan:
			client.newChan(msg)
		case common.MsgTypeDelChan:
			client.delChan(msg)
		default:
      client.sendMsg(common.MsgTypeErr, common.ErrBadMsg.Error())
		}
	}
}

func (s *Server) newClient(conn net.Conn) *Client {
	return &Client{
    server: s,
		conn: conn,
		subs: common.NewSyncSet[string](),
		pubs: common.NewSyncSet[string](),
	}
}

type Client struct {
  server *Server
	conn net.Conn
	subs *common.SyncSet[string]
	pubs *common.SyncSet[string]
}

func (c *Client) publish(msg string) {
	// Get the channel names and the message
	parts := strings.SplitN(msg, "\r", 2)
	if len(parts) != 2 {
		c.sendMsg(common.MsgTypeErr, common.ErrBadMsg.Error())
		return
	}
	if parts[0] == "" {
		c.pubs.Range(func(name string) bool {
			channel, loaded := c.server.channels.Load(name)
			if loaded {
				channel.publish(parts[1])
			} else {
				c.server.errorLog.Printf("should have channel %s but not in channels", name)
			}
			return true
		})
		c.sendMsg(common.MsgTypeOk, "")
		return
	}
	// Send the message to each channel the client owns
	for _, name := range strings.Split(parts[0], "\n") {
		if c.pubs.Contains(name) {
			channel, loaded := c.server.channels.Load(name)
			// TODO: Possibly send something if not loaded
			if loaded {
				channel.publish(parts[1])
			} else {
				c.server.errorLog.Printf("should have channel %s but not in channels", name)
			}
		}
	}
	c.sendMsg(common.MsgTypeOk, "")
}

func (c *Client) sub(msg string) {
  var fails []string
	for _, name := range strings.Split(msg, "\n") {
		if !c.addSub(name) {
			fails = append(fails, name)
		}
	}
	if len(fails) != 0 {
		c.sendMsg(common.MsgTypeSub, strings.Join(fails, "\n"))
		return
	}
	c.sendMsg(common.MsgTypeOk, "")
}

func (c *Client) unsub(msg string) {
	if msg == "" {
		c.subs.Range(func(name string) bool {
			c.delSub(name)
			return true
		})
		c.sendMsg(common.MsgTypeOk, "")
		return
	}
	for _, name := range strings.Split(msg, "\n") {
		c.delSub(name)
	}
	c.sendMsg(common.MsgTypeOk, "")
}

func (c *Client) newChan(msg string) {
  var fails []string
	for _, name := range strings.Split(msg, "\n") {
		if c.pubs.Contains(name) {
			continue
		}
		if _, loaded := c.server.channels.LoadOrStore(name, newChannel(name)); loaded {
			fails = append(fails, name)
			continue
		}
		c.pubs.Insert(name)
	}
	if len(fails) != 0 {
		c.sendMsg(common.MsgTypeNewChan, strings.Join(fails, "\n"))
		return
	}
	c.sendMsg(common.MsgTypeOk, "")
}

func (c *Client) delChan(msg string) {
	if msg == "" {
		c.pubs.Range(func(name string) bool {
			if channel, loaded := c.server.channels.LoadAndDelete(name); loaded {
				channel.del()
			}
			return true
		})
		c.sendMsg(common.MsgTypeOk, "")
		return
	}
	// TODO: Possibly don't sent fails if client doesn't own channel
  var fails []string
	for _, name := range strings.Split(msg, "\n") {
		if !c.pubs.Contains(name) {
      // TODO: Possibly do this if sending Unsub in channel.del()
			//fails = append(fails, name)
			continue
		}
		// TODO: Possibly send error if channel doesn't exist
		if channel, loaded := c.server.channels.LoadAndDelete(name); loaded {
			channel.del()
		}
		c.pubs.Remove(name)
	}
	if len(fails) != 0 {
    // NOTE: Unreachable when append stmt above is commented out
		c.sendMsg(common.MsgTypeErr, strings.Join(fails, "\n"))
		return
	}
	c.sendMsg(common.MsgTypeOk, "")
}

// Returns false if the channel doesn't exist
func (c *Client) addSub(s string) bool {
	// Add channel to client set
	added := c.subs.Insert(s)
	if !added {
		return true
	}
	// Get the channel from the global map
	// If it doesn't exist, remove it from the client's set
	channel, loaded := c.server.channels.Load(s)
	if !loaded {
		c.subs.Remove(s)
		return false
	}
	channel.addClient(c)
	return added
}

// Returns true if the client was subbed
func (c *Client) delSub(s string) bool {
	if !c.subs.Remove(s) {
		return false
	}
	channel, loaded := c.server.channels.Load(s)
	if loaded {
		channel.delClient(c)
	}
	return true
}

func (c *Client) sendMsg(mt common.MsgType, msg string) {
	c.conn.Write(common.EncodeMsg(mt, msg))
}

type Channel struct {
	name string
	subs *common.SyncSet[*Client]
}

func newChannel(name string) *Channel {
	return &Channel{name: name, subs: common.NewSyncSet[*Client]()}
}

func (ch *Channel) addClient(c *Client) {
	ch.subs.Insert(c)
}

func (ch *Channel) delClient(c *Client) {
	ch.subs.Remove(c)
}

func (ch *Channel) publish(msg string) {
	ch.subs.Range(func(c *Client) bool {
		c.sendMsg(common.MsgTypePub, ch.name+"\r"+msg)
		return true
	})
}

func (ch *Channel) del() {
	ch.subs.Range(func(c *Client) bool {
		c.sendMsg(common.MsgTypeDelChan, ch.name)
		return true
	})
}
