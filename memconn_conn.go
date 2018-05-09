package memconn

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"
)

// Conn is an in-memory implementation of Golang's "net.Conn" interface.
type Conn struct {
	pipe

	laddr Addr
	raddr Addr

	// buf contains information about the connection's buffer state if
	// the connection is buffered. Otherwise this field is nil.
	buf *bufConn
}

type bufConn struct {
	// Please see the SetCloseTimeout function for more information.
	timeout time.Duration

	// errs is the error channel returned by the Errs() function and
	// used to report erros that occur as a result of buffered write
	// operations.
	errs chan error

	// tx is used for buffering writes.
	tx bufTx
}

type bufTx struct {
	// Once is used by the writeAsync function to:
	//
	//   * Grow the buffer to satisfy "size" bytes
	//   * Start the goroutine in which the read loop is executed
	sync.Once

	// lock is a channel used to disallow concurrent, buffered writes.
	lock chan struct{}

	// Please see the SetWriteBuffer functions for more information.
	size int

	// data is used to buffer the Writes.
	data bytes.Buffer

	// rdRx is a channel that receives a signal when the buffer has
	// data to be read.
	rdRx chan struct{}

	// wrRx is a channel that receives a signal when the buffer has
	// free space. This channel is buffered and can accept a single
	// signal before the channel begins to block.
	wrRx chan struct{}
}

func makeNewConns(network string, laddr, raddr Addr) (*Conn, *Conn) {
	// This code is duplicated from the Pipe() function from the file
	// "memconn_pipe.go". The reason for the duplication is to optimize
	// the performance by removing the need to wrap the *pipe values as
	// interface{} objects out of the Pipe() function and assert them
	// back as *pipe* objects in this function.
	cb1 := make(chan []byte)
	cb2 := make(chan []byte)
	cn1 := make(chan int)
	cn2 := make(chan int)
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	// Wrap the pipes with Conn to support:
	//
	//   * The correct address information for the functions LocalAddr()
	//     and RemoteAddr() return the
	//   * Errors returns from the internal pipe are checked and
	//     have their internal OpError addr information replaced with
	//     the correct address information.
	//   * A channel can be setup to cause the event of the Listener
	//     closing closes the remoteConn immediately.
	//   * Buffered writes
	local := &Conn{
		pipe: pipe{
			rdRx: cb1, rdTx: cn1,
			wrTx: cb2, wrRx: cn2,
			localDone: done1, remoteDone: done2,
			readDeadline:  makePipeDeadline(),
			writeDeadline: makePipeDeadline(),
		},
		laddr: laddr,
		raddr: raddr,
	}
	remote := &Conn{
		pipe: pipe{
			rdRx: cb2, rdTx: cn2,
			wrTx: cb1, wrRx: cn1,
			localDone: done2, remoteDone: done1,
			readDeadline:  makePipeDeadline(),
			writeDeadline: makePipeDeadline(),
		},
		laddr: raddr,
		raddr: laddr,
	}

	if laddr.Buffered() {
		local.buf = &bufConn{
			errs:    make(chan error),
			timeout: 0 * time.Second,
			tx: bufTx{
				lock: make(chan struct{}, 1),
				rdRx: make(chan struct{}),
				wrRx: make(chan struct{}, 1),
			},
		}
	}

	if raddr.Buffered() {
		remote.buf = &bufConn{
			errs:    make(chan error),
			timeout: 3 * time.Second,
			tx: bufTx{
				lock: make(chan struct{}, 1),
				rdRx: make(chan struct{}),
				wrRx: make(chan struct{}, 1),
			},
		}
	}

	return local, remote
}

// LocalBuffered returns a flag indicating whether or not the local side
// of the connection is buffered.
func (c *Conn) LocalBuffered() bool {
	return c.laddr.Buffered()
}

// RemoteBuffered returns a flag indicating whether or not the remote side
// of the connection is buffered.
func (c *Conn) RemoteBuffered() bool {
	return c.raddr.Buffered()
}

// SetWriteBuffer sets the number of bytes allowed to be queued for
// buffered Writes. Once the buffer is full, Write IO is blocked until
// the buffer has free space.
//
// The default buffer size is zero, which means no maximum is defined.
//
// Setting this value has no effect after the first Write to the
// connection.
//
// Please note that setting the buffer size has no effect on unbuffered
// connections.
func (c *Conn) SetWriteBuffer(bytes int) {
	if c.buf != nil {
		c.buf.tx.size = bytes
	}
}

// SetCloseTimeout sets a time.Duration value used by the Close function
// to determine the amount of time to wait for pending, buffered Writes
// to complete before closing the connection.
//
// The default timeout value is 0 seconds for the local side of a
// connection and 3 seconds for the remote side of a connection.
//
// A zero value means the connection is closed immediately.
//
// Please note that setting this value has no effect on unbuffered
// connections.
func (c *Conn) SetCloseTimeout(duration time.Duration) {
	if c.buf != nil {
		c.buf.timeout = duration
	}
}

// LocalAddr implements the net.Conn LocalAddr method.
func (c *Conn) LocalAddr() net.Addr {
	return c.laddr
}

// RemoteAddr implements the net.Conn RemoteAddr method.
func (c *Conn) RemoteAddr() net.Addr {
	return c.raddr
}

// Close implements the net.Conn Close method.
func (c *Conn) Close() error {
	c.pipe.once.Do(func() {
		// Buffered connections will attempt to wait until all
		// pending Writes are completed or until the specified
		// timeout value has elapsed.
		if c.buf != nil {
			// Locking the buffer ensures no more buffered Writes
			// are enqueued.
			c.buf.tx.lock <- struct{}{}

			// Block until the read loop indicates it has drained
			// the buffer or until the specified timeout has elapsed.
			select {
			case <-c.buf.tx.wrRx:
			case <-time.After(c.buf.timeout):
			}
		}

		close(c.pipe.localDone)
	})
	return c.pipe.Close()
}

// Errs returns a channel that receives errors that may occur as the
// result of buffered write operations.
//
// This function will always return nil for unbuffered connections.
//
// Please note that the channel returned by this function is not closed
// when the connection is closed. This is because errors may continue
// to be sent over this channel as the result of asynchronous writes
// occurring after the connection is closed. Therefore this channel
// should not be used to determine when the connection is closed.
func (c *Conn) Errs() <-chan error {
	return c.buf.errs
}

// SetReadDeadline implements the net.Conn SetReadDeadline method.
func (c *Conn) SetReadDeadline(t time.Time) error {
	if err := c.pipe.SetReadDeadline(t); err != nil {
		if e, ok := err.(*net.OpError); ok {
			e.Addr = c.laddr
			e.Source = c.laddr
			return e
		}
		return &net.OpError{
			Op:     "setReadDeadline",
			Addr:   c.laddr,
			Source: c.laddr,
			Net:    c.laddr.network,
			Err:    err,
		}
	}
	return nil
}

// SetWriteDeadline implements the net.Conn SetWriteDeadline method.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	if err := c.pipe.SetWriteDeadline(t); err != nil {
		if e, ok := err.(*net.OpError); ok {
			e.Addr = c.laddr
			e.Source = c.laddr
			return e
		}
		return &net.OpError{
			Op:     "setWriteDeadline",
			Addr:   c.laddr,
			Source: c.laddr,
			Net:    c.laddr.network,
			Err:    err,
		}
	}
	return nil
}

// Read implements the net.Conn Read method.
func (c *Conn) Read(b []byte) (int, error) {
	n, err := c.pipe.Read(b)
	if err != nil {
		if e, ok := err.(*net.OpError); ok {
			e.Addr = c.raddr
			e.Source = c.laddr
			return n, e
		}
		return n, &net.OpError{
			Op:     "read",
			Addr:   c.raddr,
			Source: c.laddr,
			Net:    c.raddr.network,
			Err:    err,
		}
	}
	return n, nil
}

// Write implements the net.Conn Write method.
func (c *Conn) Write(p []byte) (int, error) {
	if c.buf != nil {
		return c.writeAsync(p)
	}
	return c.writeSync(p)
}

func (c *Conn) writeSync(p []byte) (int, error) {
	n, err := c.pipe.Write(p)
	if err != nil {
		if e, ok := err.(*net.OpError); ok {
			e.Addr = c.raddr
			e.Source = c.laddr
			return n, e
		}
		return n, &net.OpError{
			Op:     "write",
			Addr:   c.raddr,
			Source: c.laddr,
			Net:    c.raddr.network,
			Err:    err,
		}
	}
	return n, nil
}

// writeAsync performs the Write operation in a goroutine. This
// behavior means the Write operation is not blocking, but also means
// that when Write operations fail the associated error is not returned
// from this function.
func (c *Conn) writeAsync(p []byte) (int, error) {
	// Block until exclusive access to the buffer is granted or until
	// the local side of the connection has been closed.
	select {
	case c.buf.tx.lock <- struct{}{}:
		defer func() { <-c.buf.tx.lock }()
	case <-c.localDone:
		return 0, &net.OpError{
			Op:     "write",
			Addr:   c.raddr,
			Source: c.laddr,
			Net:    c.raddr.network,
			Err:    io.ErrClosedPipe,
		}
	}

	// On the first Write do the following exactly once:
	//   * Grow the buffer to satisify the specified maximum.
	//   * Start the loop that reads from the buffer and writes to
	//     the underlying pipe.
	c.buf.tx.Once.Do(func() {
		c.buf.tx.data.Grow(c.buf.tx.size)
		go c.readBufferWriteToPipeLoop()
	})

	var buffered int

	// Write the payload to the buffer until the number of bytes that
	// have been buffered equals the size of the original payload.
	for len(p) > 0 {

		// Block until the buffer has free space or until the remote
		// side of the connection has been closed.
		select {
		case <-c.buf.tx.rdRx:
		case <-c.remoteDone:
			return buffered, &net.OpError{
				Op:     "write",
				Addr:   c.raddr,
				Source: c.laddr,
				Net:    c.raddr.network,
				Err:    io.ErrClosedPipe,
			}
		}

		// b2b is the number of bytes-to-buffer
		var b2b int

		// If the buffer has no set maximum or if its capacity is
		// larger than len(b), buffer all of b at once. Otherwise
		// fill the buffer to its capacity.
		if c.buf.tx.size == 0 || c.buf.tx.data.Cap() > len(p) {
			b2b = len(p)
		} else {
			b2b = c.buf.tx.data.Cap()
		}

		// Buffer the data.
		n, err := c.buf.tx.data.Write(p[:b2b])
		buffered = buffered + n
		if err != nil {
			return buffered, &net.OpError{
				Op:     "write",
				Addr:   c.raddr,
				Source: c.laddr,
				Net:    c.raddr.network,
				Err:    err,
			}
		}

		// Remove the number of bytes buffered from the front of
		// the payload.
		p = p[n:]

		// Notify the read loop that the buffer has data.
		c.buf.tx.wrRx <- struct{}{}
	}

	return buffered, nil
}

func (c *Conn) readBufferWriteToPipeLoop() {
	for {
		// Block until announcing the buffer has free space or until
		// the remote side of the connection has been closed.
		select {
		case c.buf.tx.rdRx <- struct{}{}:
		case <-c.remoteDone:
			go func() {
				c.buf.errs <- &net.OpError{
					Op:     "write",
					Addr:   c.raddr,
					Source: c.laddr,
					Net:    c.raddr.network,
					Err:    io.ErrClosedPipe,
				}
			}()
			return
		}

		// Block until there is data in the buffer or until the
		// remote side of the connection has been closed.
		select {
		case <-c.buf.tx.wrRx:
		case <-c.remoteDone:
			go func() {
				c.buf.errs <- &net.OpError{
					Op:     "write",
					Addr:   c.raddr,
					Source: c.laddr,
					Net:    c.raddr.network,
					Err:    io.ErrClosedPipe,
				}
			}()
			return
		}

		// Skip the write if there are no unread bytes in the buffer.
		if c.buf.tx.data.Len() == 0 {
			continue
		}

		// Drain the buffer into the underlying pipe. The buffer's
		// WriteTo function is used because it writes the unread
		// bytes directly to the destination (https://goo.gl/ebKihy).
		// This ensures there is never more data held in memory than
		// the specified buffer maximum, the remote side of the
		// connection notwithstanding.
		if _, err := c.buf.tx.data.WriteTo(&c.pipe); err != nil {
			go func() {
				c.buf.errs <- &net.OpError{
					Op:     "write",
					Addr:   c.raddr,
					Source: c.laddr,
					Net:    c.raddr.network,
					Err:    err,
				}
			}()
		}
	}
}
