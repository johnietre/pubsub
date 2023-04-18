package pubsub

import (
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"

	"github.com/johnietre/pubsub/utils"
)

const (
	bufSize = 1024
)

// Server acts as a server/hub for clients and channels
type Server struct {
	// TODO: Add Closed variable to keep track of if closed to return err from
	// NewClient?
	ln       net.Listener
	channels *utils.SyncMap[string, *serverChannel]
	errorLog *log.Logger
}

// NewServer creates a new server listening on the given address
func NewServer(addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{
		ln:       ln,
		channels: utils.NewSyncMap[string, *serverChannel](),
		errorLog: log.Default(),
	}, nil
}

// NewServerFromLn creates a new server with the given listener
func NewServerFromLn(ln net.Listener) *Server {
	return &Server{
		ln:       ln,
		channels: utils.NewSyncMap[string, *serverChannel](),
		errorLog: log.Default(),
	}
}

// NewLocalServer creates a new local server with clients only being able to be
// created using the NewClient method on the server
func NewLocalServer() *Server {
	return &Server{
		ln:       nil,
		channels: utils.NewSyncMap[string, *serverChannel](),
		errorLog: log.Default(),
	}
}

// Run actually runs the server
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

// Shutdown calls the close method on the underlying listener, not closing any
// active connections
func (s *Server) Shutdown() error {
	if s.ln == nil {
		return nil
	}
	return s.ln.Close()
}

// SetErrorLog sets the logger used by the server
func (s *Server) SetErrorLog(logger *log.Logger) {
	s.errorLog = logger
}

// GetErrorLog gets the logger used by the server
func (s *Server) GetErrorLog() *log.Logger {
	return s.errorLog
}

// NewClient creates a new client piped to the server (using net.Pipe)
func (s *Server) NewClient(queueLen uint32, discard bool) *Client {
	clientConn, serverConn := net.Pipe()
	go s.handle(serverConn)
	return NewClientFromConn(clientConn, queueLen, discard)
}

func (s *Server) handle(c net.Conn) {
	client := s.newServerClient(c)
	defer c.Close()
	defer func() {
		client.unsub("")
		client.delChan("")
	}()
	for {
		initial := make([]byte, 5)
		n, err := c.Read(initial[:])
		if err != nil {
			if err != io.EOF {
				s.errorLog.Println(err)
			}
			return
		}
		if n != 5 {
			client.sendMsg(utils.MsgTypeErr, utils.ErrBadMsg.Error())
			continue
		}
		n = int(utils.Get4(initial[1:]))
		buf := make([]byte, n)
		if n != 0 {
			n, err = c.Read(buf)
			if err != nil {
				return
			}
		}
		mt, msg, err := utils.DecodeMsg(append(initial, buf[:n]...))
		if err != nil {
			client.sendMsg(utils.MsgTypeErr, err.Error())
			continue
		}
		switch mt {
		case utils.MsgTypePub:
			client.publish(msg)
		case utils.MsgTypeSub:
			client.sub(msg)
		case utils.MsgTypeUnsub:
			client.unsub(msg)
		case utils.MsgTypeNewChan:
			client.newChan(msg)
		case utils.MsgTypeNewMultiChan:
			client.newMultiChan(msg)
		case utils.MsgTypeDelChan:
			client.delChan(msg)
		case utils.MsgTypeChanNames:
			client.chanNames()
		default:
			client.sendMsg(utils.MsgTypeErr, utils.ErrBadMsg.Error())
		}
	}
}

func (s *Server) newServerClient(conn net.Conn) *serverClient {
	return &serverClient{
		server: s,
		conn:   conn,
		subs:   utils.NewSyncSet[string](),
		pubs:   utils.NewSyncSet[string](),
	}
}

func (s *Server) newServerChannel(name string) *serverChannel {
	return &serverChannel{
		name:   name,
		server: s,
		subs:   utils.NewSyncSet[*serverClient](),
	}
}

type serverClient struct {
	server *Server
	conn   net.Conn
	subs   *utils.SyncSet[string]
	pubs   *utils.SyncSet[string]
}

func (c *serverClient) publish(msg string) {
	// Get the channel names and the message
	parts := strings.SplitN(msg, "\r", 2)
	if len(parts) != 2 {
		c.sendMsg(utils.MsgTypeErr, utils.ErrBadMsg.Error())
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
		c.sendMsg(utils.MsgTypeOk, "")
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
	c.sendMsg(utils.MsgTypeOk, "")
}

func (c *serverClient) sub(msg string) {
	var fails []string
	names := strings.Split(msg, "\n")
	for _, name := range names {
		if !c.addSub(name) {
			fails = append(fails, name)
		}
	}
	if len(fails) != 0 {
		c.sendMsg(utils.MsgTypeSub, strings.Join(fails, "\n"))
		return
	}
	c.sendMsg(utils.MsgTypeOk, "")
}

func (c *serverClient) unsub(msg string) {
	if msg == "" {
		c.subs.Range(func(name string) bool {
			c.delSub(name)
			return true
		})
		c.sendMsg(utils.MsgTypeOk, "")
		return
	}
	for _, name := range strings.Split(msg, "\n") {
		c.delSub(name)
	}
	c.sendMsg(utils.MsgTypeOk, "")
}

func (c *serverClient) newChan(msg string) {
	var fails []string
	for _, name := range strings.Split(msg, "\n") {
		if c.pubs.Contains(name) {
			continue
		}
		if _, loaded := c.server.channels.LoadOrStore(name, c.server.newServerChannel(name)); loaded {
			fails = append(fails, name)
			continue
		}
		c.pubs.Insert(name)
	}
	if len(fails) != 0 {
		c.sendMsg(utils.MsgTypeNewChan, strings.Join(fails, "\n"))
		return
	}
	c.sendMsg(utils.MsgTypeOk, "")
}

func (c *serverClient) newMultiChan(msg string) {
	var fails []string
	for _, name := range strings.Split(msg, "\n") {
		if c.pubs.Contains(name) {
			continue
		}
		channel := c.server.newServerChannel(name)
		channel.numPubs = 1
	RetryMulti:
		if channel, loaded := c.server.channels.LoadOrStore(name, channel); loaded {
			// Check the state of the channel
			n := atomic.LoadInt32(&channel.numPubs)
			// If -1, the channel is single pub
			if n == -1 {
				fails = append(fails, name)
				continue
			}
			// If n is less than 1 or the new numPubs is less than 2, the channel has been deleted
			if n < 1 || atomic.AddInt32(&channel.numPubs, 1) < 2 {
				// Try again
				goto RetryMulti
			}
		}
		c.pubs.Insert(name)
	}
	if len(fails) != 0 {
		c.sendMsg(utils.MsgTypeNewMultiChan, strings.Join(fails, "\n"))
		return
	}
	c.sendMsg(utils.MsgTypeOk, "")
}

func (c *serverClient) delChan(msg string) {
	if msg == "" {
		c.pubs.Range(func(name string) bool {
			if channel, loaded := c.server.channels.Load(name); loaded {
				channel.del()
			}
			c.pubs.Remove(name)
			return true
		})
		c.sendMsg(utils.MsgTypeOk, "")
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
		if channel, loaded := c.server.channels.Load(name); loaded {
			channel.del()
		}
		c.pubs.Remove(name)
	}
	if len(fails) != 0 {
		// NOTE: Unreachable when append stmt above is commented out
		c.sendMsg(utils.MsgTypeErr, strings.Join(fails, "\n"))
		return
	}
	c.sendMsg(utils.MsgTypeOk, "")
}

func (c *serverClient) chanNames() {
	var names []string
	c.server.channels.Range(func(name string, _ *serverChannel) bool {
		names = append(names, name)
		return true
	})
	c.sendMsg(utils.MsgTypeChanNames, strings.Join(names, "\n"))
}

// Returns false if the channel doesn't exist
func (c *serverClient) addSub(s string) bool {
	// Add channel to client set
	added := c.subs.Insert(s)
	if !added {
		return true
	}
	// Get the channel from the server map
	// If it doesn't exist, remove it from the client's set
	channel, loaded := c.server.channels.Load(s)
	if !loaded {
		c.subs.Remove(s)
		return false
	}
	// Add the client to the channel's list, removing it from the client's list
	// if it no longer exists
	if !channel.addServerClient(c) {
		c.subs.Remove(s)
		return false
	}
	return true
}

// Returns true if the client was subbed
func (c *serverClient) delSub(s string) bool {
	if !c.subs.Remove(s) {
		return false
	}
	channel, loaded := c.server.channels.Load(s)
	if loaded {
		channel.delServerClient(c)
	}
	return true
}

func (c *serverClient) sendMsg(mt utils.MsgType, msg string) error {
	_, err := c.conn.Write(utils.EncodeMsg(mt, msg))
	return err
}

type serverChannel struct {
	name   string
	server *Server
	subs   *utils.SyncSet[*serverClient]
	// The number of publishers the channel has.
	// This is -1 if the channel only has 1 publisher and > 0 if multiple.
	// It's -2 if the channel is deleted
	// TODO: Possibly remove the -2 state conditon
	numPubs int32
}

// Returns true if the client was added
func (ch *serverChannel) addServerClient(c *serverClient) bool {
	ch.subs.Insert(c)
	// Check to see if the channel has been deleted
	n := atomic.LoadInt32(&ch.numPubs)
	return n == -2 || n == 0
}

func (ch *serverChannel) delServerClient(c *serverClient) {
	ch.subs.Remove(c)
}

func (ch *serverChannel) publish(msg string) {
	ch.subs.Range(func(c *serverClient) bool {
		c.sendMsg(utils.MsgTypePub, ch.name+"\r"+msg)
		return true
	})
}

// Returns the decremented numPubs value
// TODO: The return unneed right now
func (ch *serverChannel) del() int32 {
	// Return if the channel is multipub and there are still publishers
	n := atomic.AddInt32(&ch.numPubs, -1)
	if n > 0 {
		return n
	}
	atomic.StoreInt32(&ch.numPubs, -2)
	ch.server.channels.Delete(ch.name)
	ch.subs.Range(func(c *serverClient) bool {
		c.subs.Remove(ch.name)
		c.sendMsg(utils.MsgTypeDelChan, ch.name)
		return true
	})
	// Delete the channel from the servers list
	return -2
}
