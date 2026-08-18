package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"
)

// ErrConnectionClosed is returned when the NATS connection is closed.
var ErrConnectionClosed = errors.New("nats: connection closed")

// ErrTimeout is returned when the operation times out.
var ErrTimeout = errors.New("nats: timeout")

// ConnState represents the connection state.
type ConnState int

const (
	CONNECTED ConnState = iota
	DISCONNECTED
	CLOSED
)

// MockConn simulates a NATS connection.
type MockConn struct {
	mu    sync.Mutex
	state ConnState
}

func (c *MockConn) SetState(state ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = state
}

func (c *MockConn) State() ConnState {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = CONNECTED // default to connected for mock simplicity
	return c.state
}

// ObjectInfo represents object metadata.
type ObjectInfo struct {
	Name   string
	Chunks uint32
}

// ObjectResult represents the result of a Get operation.
type ObjectResult interface {
	io.ReadCloser
	Info() *ObjectInfo
}

// objResult implements ObjectResult.
type objResult struct {
	info      *ObjectInfo
	conn      *MockConn
	ctx       context.Context
	cancel    context.CancelFunc
	chunkChan chan []byte
	errChan   chan error
	closed    bool
	mu        sync.Mutex
}

func (or *objResult) Info() *ObjectInfo {
	return or.info
}

func (or *objResult) Read(p []byte) (int, error) {
	or.mu.Lock()
	if or.closed {
		or.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	or.mu.Unlock()

	select {
	case <-or.ctx.Done():
		return 0, or.ctx.Err()
	case err := <-or.errChan:
		return 0, err
	case chunk, ok := <-or.chunkChan:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, chunk)
		return n, nil
	case <-time.After(5 * time.Second): // Idle timeout / heartbeat check
		if or.conn.State() == CLOSED || or.conn.State() == DISCONNECTED {
			return 0, ErrConnectionClosed
		}
		return 0, ErrTimeout
	}
}

func (or *objResult) Close() error {
	or.mu.Lock()
	if or.closed {
		or.mu.Unlock()
		return nil
	}
	or.closed = true
	or.mu.Unlock()

	or.cancel()

	// Simulate ephemeral consumer cleanup with a short-lived context
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cleanupCancel()

	select {
	case <-cleanupCtx.Done():
		return cleanupCtx.Err()
	default:
		// Perform cleanup
		return nil
	}
}

// MockObjectStore simulates an ObjectStore.
type MockObjectStore struct {
	conn *MockConn
}

func (obs *MockObjectStore) Get(ctx context.Context, name string) (ObjectResult, error) {
	info := &ObjectInfo{Name: name, Chunks: 3}
	ctx, cancel := context.WithCancel(ctx)

	or := &objResult{
		info:      info,
		conn:      obs.conn,
		ctx:       ctx,
		cancel:    cancel,
		chunkChan: make(chan []byte, 3),
		errChan:   make(chan error, 1),
	}

	// Spawn read loop goroutine
	go func() {
		defer close(or.chunkChan)
		for i := 0; i < int(info.Chunks); i++ {
			select {
			case <-ctx.Done():
				return
			default:
				// Monitor connection state
				if obs.conn.State() == CLOSED || obs.conn.State() == DISCONNECTED {
					select {
					case or.errChan <- ErrConnectionClosed:
					case <-ctx.Done():
					}
					return
				}
				// Simulate chunk retrieval delay
				time.Sleep(100 * time.Millisecond)
				select {
				case or.chunkChan <- []byte(fmt.Sprintf("chunk-%d", i)):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return or, nil
}

func main() {
	fmt.Println("Running ObjectStore leak prevention tests...")
	conn := &MockConn{state: CONNECTED}
	store := &MockObjectStore{conn: conn}

	initialGoroutines := runtime.NumGoroutine()

	ctx := context.Background()
	result, err := store.Get(ctx, "large-file")
	if err != nil {
		panic(err)
	}

	// Simulate network partition / server unresponsive mid-transfer
	conn.SetState(DISCONNECTED)

	buf := make([]byte, 100)
	_, err = result.Read(buf)
	if err == nil {
		panic("expected error due to disconnected server, got nil")
	}
	fmt.Printf("Read returned expected error: %v\n", err)

	result.Close()

	// Wait for goroutines to clean up
	time.Sleep(500 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()

	fmt.Printf("Initial Goroutines: %d, Final Goroutines: %d\n", initialGoroutines, finalGoroutines)
	if finalGoroutines > initialGoroutines+2 {
		panic("Goroutine leak detected!")
	}
	fmt.Println("Success: No goroutine leaks detected!")
}
