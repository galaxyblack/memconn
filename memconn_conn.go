package memconn

import (
	"bytes"
	"fmt"
	"io"
	"log"
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

	isRemote bool
}

type bufConn struct {

	// Please see the SetBufferSize function for more information.
	max      int
	growOnce sync.Once

	// Please see the SetCloseTimeout function for more information.
	closeTimeout time.Duration

	// errs is the error channel returned by the Errs() function and
	// used to report erros that occur as a result of buffered write
	// operations. If the pipe does not use buffered writes then this
	// field will always be nil.
	errs chan error

	// data is a circular buffer used to provide buffered writes
	data bytes.Buffer

	// configMu is used to synchronize access to buffered connection
	// settings.
	configMu sync.RWMutex

	// sigPrev is a signal that buffered write goroutines use to
	// block until the previous write has completed.
	sigPrev chan struct{}

	// wait is used to track the enqueue activity of buffered Writes.
	// wait is used by the Close function to block until all enqueue
	// attempts have completed.
	wait sync.WaitGroup

	cond sync.Cond

	done chan struct{}
	lock chan struct{}
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
		laddr:    raddr,
		raddr:    laddr,
		isRemote: true,
	}

	if laddr.Buffered() {
		local.buf = &bufConn{
			max:          64,
			errs:         make(chan error),
			closeTimeout: 0 * time.Second,
			sigPrev:      make(chan struct{}),
			done:         make(chan struct{}),
			lock:         make(chan struct{}, 1),
		}
		local.buf.cond.L = &sync.Mutex{}
		close(local.buf.sigPrev)
	}

	if raddr.Buffered() {
		remote.buf = &bufConn{
			max:          64,
			errs:         make(chan error),
			closeTimeout: 3 * time.Second,
			sigPrev:      make(chan struct{}),
			done:         make(chan struct{}),
			lock:         make(chan struct{}, 1),
		}
		remote.buf.cond.L = &sync.Mutex{}
		close(remote.buf.sigPrev)
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

// BufferSize gets the number of bytes allowed to be queued for
// asynchrnous Write operations.
//
// Please note that this function will always return zero for unbuffered
// connections.
//
// Please see the function SetBufferSize for more information.
func (c *Conn) BufferSize() int {
	if c.laddr.Buffered() {
		c.buf.configMu.RLock()
		defer c.buf.configMu.RUnlock()
		return c.buf.max
	}
	return 0
}

// SetBufferSize sets the number of bytes allowed to be queued for
// asynchronous Write operations. Once the amount of data pending a Write
// operation exceeds the specified size, subsequent Writes will
// block until the queued data no longer exceeds the allowed ceiling.
//
// A value of zero means no maximum is defined.
//
// If a Write operation's payload length exceeds the buffer size
// (except for zero) then the Write operation is handled synchronously.
//
// Please note that setting the buffer size has no effect on unbuffered
// connections.
func (c *Conn) SetBufferSize(i int) {
	if c.laddr.Buffered() {
		c.buf.configMu.Lock()
		defer c.buf.configMu.Unlock()
		c.buf.max = i
	}
}

// CloseTimeout gets the time.Duration value used when closing buffered
// connections.
//
// Please note that this function will always return zero for
// unbuffered connections.
//
// Please see the function SetCloseTimeout for more information.
func (c *Conn) CloseTimeout() time.Duration {
	if c.laddr.Buffered() {
		c.buf.configMu.RLock()
		defer c.buf.configMu.RUnlock()
		return c.buf.closeTimeout
	}
	return 0
}

// SetCloseTimeout sets a time.Duration value used by the Close function
// to determine the amount of time to wait for pending, buffered Writes
// to complete before closing the connection.
//
// The default timeout value is 10 seconds. A zero value does not
// mean there is no timeout, rather it means the timeout is immediate.
//
// Please note that setting this value has no effect on unbuffered
// connections.
func (c *Conn) SetCloseTimeout(d time.Duration) {
	if c.laddr.Buffered() {
		c.buf.configMu.Lock()
		defer c.buf.configMu.Unlock()
		c.buf.closeTimeout = d
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
		if c.laddr.Buffered() {

			// Set up a channel that is closed when the specified
			// timer elapses.
			timeoutDone := make(chan struct{})
			if t := c.CloseTimeout(); t == 0 {
				close(timeoutDone)
			} else {
				time.AfterFunc(t, func() { close(timeoutDone) })
			}

			// Set up a channel that is closed when all enqueue
			// operations have completed.
			go func() {
				c.buf.lock <- struct{}{}
				c.buf.wait.Wait()
				close(c.buf.done)
			}()

			// Wait to close the connection.
			select {
			case <-c.buf.done:
			case <-timeoutDone:
			}
		}

		close(c.pipe.localDone)
	})
	return nil
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
			Net:    c.laddr.Network(),
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
			Net:    c.laddr.Network(),
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
			Net:    c.raddr.Network(),
			Err:    err,
		}
	}
	return n, nil
}

// Write implements the net.Conn Write method.
func (c *Conn) Write(b []byte) (int, error) {
	if c.laddr.Buffered() {
		return c.writeAsync(b)
	}
	return c.writeSync(b)
}

func (c *Conn) writeSync(b []byte) (int, error) {
	n, err := c.pipe.Write(b)
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
			Net:    c.raddr.Network(),
			Err:    err,
		}
	}
	return n, nil
}

// writeAsync performs the Write operation in a goroutine. This
// behavior means the Write operation is not blocking, but also means
// that when Write operations fail the associated error is not returned
// from this function.
func (c *Conn) writeAsync(b []byte) (int, error) {
	// Block until either the buffer's done channel is closed or until
	// it's okay to write.
	select {
	case <-c.buf.done:
		return 0, io.ErrClosedPipe
	case c.buf.lock <- struct{}{}:
		defer func() { <-c.buf.lock }()
		// Proceed
	}

	c.buf.growOnce.Do(func() {
		c.buf.data.Grow(c.buf.max)
	})

	// Store the payload's size since the payload may be altered.
	lb := len(b)

	// nb is the total number of bytes that have been buffered
	var nb int

	// Write the payload to the buffer until the number of bytes that
	// have been buffered equals the size of the original payload.
	for nb < lb {

		// Buffer as much data as possible.
		nbw, err := c.writeBuffer(b)
		nb = nb + nbw

		if !c.isRemote {
			log.Printf("%05d %05d %05d", lb, nb, len(b))
		}

		if err != nil {
			return nb, err
		}

		// Remove the number of bytes buffered from the front of
		// the payload.
		b = b[nbw:]

		// Get the previous signal. This is used to guarantee ordered
		// Writes by blocking this Write op until the previous Write op
		// has completed.
		sigPrev := c.buf.sigPrev

		// Create a new signal to track this buffered Write. The
		// channel is buffered for two signals:
		//
		//    * Signal 1 - The buffer has been read
		//    * Signal 2 - The buffered data has been written to the pipe
		sigDone := make(chan struct{}, 1)

		// Assign the new signal to the connection's previous signal field.
		c.buf.sigPrev = sigDone

		// Start a goroutine to perform the write asynchronously.
		go c.readBufferAndWritePipe(nbw, sigPrev, sigDone)
	}

	return nb, nil
}

// doesBufferHaveFreeSpace sets the cap and free values to a buffer's
// current capacity and free space. free is set to -1 if the
// buffer has no set capacity. A true value is returned if
// free < 0 or free > 0.
func (c *Conn) doesBufferHaveFreeSpace(cap, free *int) bool {
	if *cap = c.buf.data.Cap(); *cap == 0 {
		*free = -1
	} else {
		*free = *cap - c.buf.data.Len()
	}
	return *free < 0 || *free > 0
}

func (c *Conn) writeBuffer(b []byte) (int, error) {
	// The wait group tracks the number of buffer attempts, not the
	// actual Write.
	c.buf.wait.Add(1)

	// Wait until there is free space in the buffer to proceed.
	var cap int
	var free int
	c.buf.cond.L.Lock()
	for !c.doesBufferHaveFreeSpace(&cap, &free) {
		c.buf.cond.Wait()
	}
	defer c.buf.cond.L.Unlock()

	// If the amount of free space is greater than the payload the
	// entire payload can be buffered at once. Otherwise buffer
	// only the number of bytes equal to the free space.
	if free < 0 || free > len(b) {
		free = len(b)
	}

	// Write as much data as there is free space.
	return c.buf.data.Write(b[:free])
}

func (c *Conn) readBuffer(nbw int) ([]byte, error) {
	c.buf.cond.L.Lock()
	defer c.buf.cond.L.Unlock()

	// Indicate the buffer has been read.
	defer c.buf.wait.Done()

	// Wake up a writeBuffer call that is waiting on free space.
	defer c.buf.cond.Signal()

	b := make([]byte, nbw)
	if nbr, err := c.buf.data.Read(b); err != nil {
		return nil, err
	} else if nbr < nbw {
		return nil, fmt.Errorf("trunc read: exp=%d act=%d", nbw, nbr)
	}

	return b, nil
}

func (c *Conn) readBufferAndWritePipe(nbw int, sigPrev, sigDone chan struct{}) {
	// Wait on the previous task's first signal, indicating
	// the previous task has read the buffer.
	<-sigPrev

	// Read the buffered data.
	b, err := c.readBuffer(nbw)

	// Signal that this goroutine has read the buffer.
	sigDone <- struct{}{}

	// Ensure that second signal is sent when the buffered
	// data has been written to the underlying pipe.
	defer close(sigDone)

	if err != nil {
		go func() { c.buf.errs <- err }()
		return
	}

	// Do not proceed until the previous task has written its data to
	// the underlying pipe. If the remote side of the connection has
	// been closed then return without writing the data since there will
	// be no point in doing so.
	select {
	case <-sigPrev:
	case <-c.remoteDone:
		return
	}

	// Write the temporary buffer into the underlying connection.
	nw, err := c.writeSync(b)
	if err != nil {
		go func() { c.buf.errs <- err }()
		return
	}

	if !c.isRemote {
		log.Printf("%05d", nw)
	}

	if nw != nbw {
		err := fmt.Errorf("trunc write: exp=%d act=%d", nbw, nw)
		go func() { c.buf.errs <- err }()
	}
}
