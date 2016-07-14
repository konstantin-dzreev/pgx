package pgx

import (
	"errors"
	"sync"
	"time"
)

type RsConnPool struct {
	allConnections       []*Conn
	availableConnections []*Conn
	cond                 *sync.Cond
	config               ConnConfig // config used when establishing connection
	maxConnections       int
	resetCount           int
	afterConnect         func(*Conn) error
	logger               Logger
	logLevel             int
	closed               bool
	acquireTimeout       time.Duration
	pgTypes              map[Oid]PgType
	pgsql_af_inet        *byte
	pgsql_af_inet6       *byte
}

// NewRsConnPool creates a new RsConnPool. config.ConnConfig is passed through to
// Connect directly.
func NewRsConnPool(config ConnPoolConfig) (p *RsConnPool, err error) {
	p = new(RsConnPool)
	p.config = config.ConnConfig
	p.maxConnections = config.MaxConnections
	if p.maxConnections == 0 {
		p.maxConnections = 5
	}
	if p.maxConnections < 1 {
		return nil, errors.New("MaxConnections must be at least 1")
	}
	p.acquireTimeout = config.AcquireTimeout
	if p.acquireTimeout < 0 {
		return nil, errors.New("AcquireTimeout must be equal to or greater than 0")
	}

	p.afterConnect = config.AfterConnect

	if config.LogLevel != 0 {
		p.logLevel = config.LogLevel
	} else {
		// Preserve pre-LogLevel behavior by defaulting to LogLevelDebug
		p.logLevel = LogLevelDebug
	}
	p.logger = config.Logger
	if p.logger == nil {
		p.logLevel = LogLevelNone
	}

	p.allConnections = make([]*Conn, 0, p.maxConnections)
	p.availableConnections = make([]*Conn, 0, p.maxConnections)
	p.cond = sync.NewCond(new(sync.Mutex))

	// Initially establish one connection
	var c *Conn
	c, err = p.createConnection()
	if err != nil {
		return
	}
	p.allConnections = append(p.allConnections, c)
	p.availableConnections = append(p.availableConnections, c)

	return
}

// Acquire takes exclusive use of a connection until it is released.
func (p *RsConnPool) Acquire() (*Conn, error) {
	p.cond.L.Lock()
	c, err := p.acquire(nil)
	p.cond.L.Unlock()
	return c, err
}

// acquire performs acquision assuming pool is already locked
func (p *RsConnPool) acquire(deadline *time.Time) (*Conn, error) {
	if p.closed {
		return nil, errors.New("cannot acquire from closed pool")
	}

	// A connection is available
	if len(p.availableConnections) > 0 {
		c := p.availableConnections[len(p.availableConnections)-1]
		c.poolResetCount = p.resetCount
		p.availableConnections = p.availableConnections[:len(p.availableConnections)-1]
		return c, nil
	}

	// No connections are available, but we can create more
	if len(p.allConnections) < p.maxConnections {
		c, err := p.createConnection()
		if err != nil {
			return nil, err
		}
		c.poolResetCount = p.resetCount
		p.allConnections = append(p.allConnections, c)
		return c, nil
	}

	// All connections are in use and we cannot create more
	if p.logLevel >= LogLevelWarn {
		p.logger.Warn("All connections in pool are busy - waiting...")
	}

	// Set initial timeout/deadline value. If the method (acquire) happens to
	// recursively call itself the deadline should retain its value.
	if deadline == nil && p.acquireTimeout > 0 {
		tmp := time.Now().Add(p.acquireTimeout)
		deadline = &tmp
	}
	// If there is a deadline then start a timeout timer
	if deadline != nil {
		timer := time.AfterFunc(deadline.Sub(time.Now()), func() {
			p.cond.Signal()
		})
		defer timer.Stop()
	}

	// Wait until there is an available connection OR room to create a new connection
	for len(p.availableConnections) == 0 && len(p.allConnections) == p.maxConnections {
		if deadline != nil && time.Now().After(*deadline) {
			return nil, errors.New("Timeout: All connections in pool are busy")
		}
		p.cond.Wait()
	}

	return p.acquire(deadline)
}

// Release gives up use of a connection.
func (p *RsConnPool) Release(conn *Conn) {
	if conn.TxStatus != 'I' {
		conn.Exec("rollback")
	}

	if len(conn.channels) > 0 {
		if err := conn.Unlisten("*"); err != nil {
			conn.die(err)
		}
		conn.channels = make(map[string]struct{})
	}
	conn.notifications = nil

	p.cond.L.Lock()

	if conn.poolResetCount != p.resetCount {
		conn.Close()
		p.cond.L.Unlock()
		p.cond.Signal()
		return
	}

	if conn.IsAlive() {
		p.availableConnections = append(p.availableConnections, conn)
	} else {
		ac := p.allConnections
		for i, c := range ac {
			if conn == c {
				ac[i] = ac[len(ac)-1]
				p.allConnections = ac[0 : len(ac)-1]
				break
			}
		}
	}
	p.cond.L.Unlock()
	p.cond.Signal()
}

// Close ends the use of a connection pool. It prevents any new connections
// from being acquired, waits until all acquired connections are released,
// then closes all underlying connections.
func (p *RsConnPool) Close() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	p.closed = true

	// Wait until all connections are released
	if len(p.availableConnections) != len(p.allConnections) {
		for len(p.availableConnections) != len(p.allConnections) {
			p.cond.Wait()
		}
	}

	for _, c := range p.allConnections {
		_ = c.Close()
	}
}

// Reset closes all open connections, but leaves the pool open. It is intended
// for use when an error is detected that would disrupt all connections (such as
// a network interruption or a server state change).
//
// It is safe to reset a pool while connections are checked out. Those
// connections will be closed when they are returned to the pool.
func (p *RsConnPool) Reset() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	p.resetCount++
	p.allConnections = make([]*Conn, 0, p.maxConnections)
	p.availableConnections = make([]*Conn, 0, p.maxConnections)
}

// Stat returns connection pool statistics
func (p *RsConnPool) Stat() (s ConnPoolStat) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	s.MaxConnections = p.maxConnections
	s.CurrentConnections = len(p.allConnections)
	s.AvailableConnections = len(p.availableConnections)
	return
}

func (p *RsConnPool) createConnection() (*Conn, error) {
	c, err := connect(p.config, p.pgTypes, p.pgsql_af_inet, p.pgsql_af_inet6)
	if err != nil {
		return nil, err
	}

	p.pgTypes = c.PgTypes
	p.pgsql_af_inet = c.pgsql_af_inet
	p.pgsql_af_inet6 = c.pgsql_af_inet6

	if p.afterConnect != nil {
		err = p.afterConnect(c)
		if err != nil {
			c.die(err)
			return nil, err
		}
	}

	return c, nil
}

// Exec acquires a connection, delegates the call to that connection, and releases the connection
func (p *RsConnPool) Exec(sql string, arguments ...interface{}) (commandTag CommandTag, err error) {
	var c *Conn
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.Exec(sql, arguments...)
}

// Query acquires a connection and delegates the call to that connection. When
// *Rows are closed, the connection is released automatically.
func (p *RsConnPool) Query(sql string, args ...interface{}) (*Rows, error) {
	c, err := p.Acquire()
	if err != nil {
		// Because checking for errors can be deferred to the *Rows, build one with the error
		return &Rows{closed: true, err: err}, err
	}

	rows, err := c.Query(sql, args...)
	if err != nil {
		p.Release(c)
		return rows, err
	}

	rows.AfterClose(p.rowsAfterClose)

	return rows, nil
}

// QueryRow acquires a connection and delegates the call to that connection. The
// connection is released automatically after Scan is called on the returned
// *Row.
func (p *RsConnPool) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := p.Query(sql, args...)
	return (*Row)(rows)
}

// Begin acquires a connection and begins a transaction on it. When the
// transaction is closed the connection will be automatically released.
func (p *RsConnPool) Begin() (*Tx, error) {
	return p.BeginIso("")
}

// BeginIso acquires a connection and begins a transaction in isolation mode iso
// on it. When the transaction is closed the connection will be automatically
// released.
func (p *RsConnPool) BeginIso(iso string) (*Tx, error) {
	for {
		c, err := p.Acquire()
		if err != nil {
			return nil, err
		}

		tx, err := c.BeginIso(iso)
		if err != nil {
			alive := c.IsAlive()
			p.Release(c)

			// If connection is still alive then the error is not something trying
			// again on a new connection would fix, so just return the error. But
			// if the connection is dead try to acquire a new connection and try
			// again.
			if alive {
				return nil, err
			}
			continue
		}

		tx.AfterClose(p.txAfterClose)
		return tx, nil
	}
}

func (p *RsConnPool) txAfterClose(tx *Tx) {
	p.Release(tx.Conn())
}

func (p *RsConnPool) rowsAfterClose(rows *Rows) {
	p.Release(rows.Conn())
}
