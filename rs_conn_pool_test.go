package pgx_test

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
)

func createRsConnPool(t *testing.T, maxConnections int) *pgx.RsConnPool {
	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: maxConnections}
	pool, err := pgx.NewRsConnPool(config)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	return pool
}

func acquireAllRsConnPoolConnections(t *testing.T, pool *pgx.RsConnPool, maxConnections int) []*pgx.Conn {
	connections := make([]*pgx.Conn, maxConnections)
	for i := 0; i < maxConnections; i++ {
		var err error
		if connections[i], err = pool.Acquire(); err != nil {
			t.Fatalf("Unable to acquire connection: %v", err)
		}
	}
	return connections
}

func releaseAllRsConnPoolConnections(pool *pgx.RsConnPool, connections []*pgx.Conn) {
	for _, c := range connections {
		pool.Release(c)
	}
}

func acquireRsConnPoolWithTimeTaken(pool *pgx.RsConnPool) (*pgx.Conn, time.Duration, error) {
	startTime := time.Now()
	c, err := pool.Acquire()
	return c, time.Now().Sub(startTime), err
}

func TestNewRsConnPool(t *testing.T) {
	t.Parallel()

	var numCallbacks int
	afterConnect := func(c *pgx.Conn) error {
		numCallbacks++
		return nil
	}

	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: 2, AfterConnect: afterConnect}
	pool, err := pgx.NewRsConnPool(config)
	if err != nil {
		t.Fatal("Unable to establish connection pool")
	}
	defer pool.Close()

	// It initially connects once
	stat := pool.Stat()
	if stat.CurrentConnections != 1 {
		t.Errorf("Expected 1 connection to be established immediately, but %v were", numCallbacks)
	}

	// Pool creation returns an error if any AfterConnect callback does
	errAfterConnect := errors.New("Some error")
	afterConnect = func(c *pgx.Conn) error {
		return errAfterConnect
	}

	config = pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: 2, AfterConnect: afterConnect}
	pool, err = pgx.NewRsConnPool(config)
	if err != errAfterConnect {
		t.Errorf("Expected errAfterConnect but received unexpected: %v", err)
	}
}

func TestNewRsConnPoolDefaultsTo5MaxConnections(t *testing.T) {
	t.Parallel()

	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig}
	pool, err := pgx.NewRsConnPool(config)
	if err != nil {
		t.Fatal("Unable to establish connection pool")
	}
	defer pool.Close()

	if n := pool.Stat().MaxConnections; n != 5 {
		t.Fatalf("Expected pool to default to 5 max connections, but it was %d", n)
	}
}

func TestRsConnPoolAcquireAndReleaseCycle(t *testing.T) {
	t.Parallel()

	maxConnections := 2
	incrementCount := int32(100)
	completeSync := make(chan int)
	pool := createRsConnPool(t, maxConnections)
	defer pool.Close()

	allConnections := acquireAllRsConnPoolConnections(t, pool, maxConnections)

	for _, c := range allConnections {
		mustExec(t, c, "create temporary table t(counter integer not null)")
		mustExec(t, c, "insert into t(counter) values(0);")
	}

	releaseAllRsConnPoolConnections(pool, allConnections)

	f := func() {
		conn, err := pool.Acquire()
		if err != nil {
			t.Fatal("Unable to acquire connection")
		}
		defer pool.Release(conn)

		// Increment counter...
		mustExec(t, conn, "update t set counter = counter + 1")
		completeSync <- 0
	}

	for i := int32(0); i < incrementCount; i++ {
		go f()
	}

	// Wait for all f() to complete
	for i := int32(0); i < incrementCount; i++ {
		<-completeSync
	}

	// Check that temp table in each connection has been incremented some number of times
	actualCount := int32(0)
	allConnections = acquireAllRsConnPoolConnections(t, pool, maxConnections)

	for _, c := range allConnections {
		var n int32
		c.QueryRow("select counter from t").Scan(&n)
		if n == 0 {
			t.Error("A connection was never used")
		}

		actualCount += n
	}

	if actualCount != incrementCount {
		fmt.Println(actualCount)
		t.Error("Wrong number of increments")
	}

	releaseAllRsConnPoolConnections(pool, allConnections)
}

func TestRsConnPoolNonBlockingConections(t *testing.T) {
	t.Parallel()

	maxConnections := 5
	openTimeout := 1 * time.Second
	dialer := net.Dialer{
		Timeout: openTimeout,
	}
	config := pgx.ConnPoolConfig{
		ConnConfig:     *defaultConnConfig,
		MaxConnections: maxConnections,
	}
	config.ConnConfig.Dial = dialer.Dial
	// We need a server that would silently DROP all incoming requests.
	// P.S. I bet there is something better than microsoft.com that does this...
	config.Host = "microsoft.com"

	pool, err := pgx.NewRsConnPool(config)
	if err == nil {
		t.Fatalf("Expected NewRsConnPool not to fail, instead it failed with")
	}

	var wg sync.WaitGroup
	wg.Add(maxConnections)

	startedAt := time.Now()
	for i := 0; i < maxConnections; i++ {
		go func() {
			_, err := pool.Acquire()
			wg.Done()
			if err == nil {
				t.Fatal("RsConnPool.Acquire() expected to fail but it did not")
			}
		}()
	}
	wg.Wait()

	timeTaken := time.Now().Sub(startedAt)
	if timeTaken > openTimeout+1*time.Second {
		t.Fatalf("Expected all RsConnPool.Aquire() to run in paralles and take about %v, instead it took '%v'", openTimeout, timeTaken)
	}

	defer pool.Close()
}

func TestRsConnAcquireTimeoutSanity(t *testing.T) {
	t.Parallel()

	config := pgx.ConnPoolConfig{
		ConnConfig:     *defaultConnConfig,
		MaxConnections: 1,
	}

	// case 1: default 0 value
	pool, err := pgx.NewRsConnPool(config)
	if err != nil {
		t.Fatalf("Expected NewRsConnPool with default config.AcquireTimeout not to fail, instead it failed with '%v'", err)
	}
	pool.Close()

	// case 2: negative value
	config.AcquireTimeout = -1 * time.Second
	_, err = pgx.NewRsConnPool(config)
	if err == nil {
		t.Fatal("Expected NewRsConnPool with negative config.AcquireTimeout to fail, instead it did not")
	}

	// case 3: positive value
	config.AcquireTimeout = 1 * time.Second
	pool, err = pgx.NewRsConnPool(config)
	if err != nil {
		t.Fatalf("Expected NewRsConnPool with positive config.AcquireTimeout not to fail, instead it failed with '%v'", err)
	}
	defer pool.Close()
}

func TestRsConnPoolWithAcquireTimeoutSet(t *testing.T) {
	t.Parallel()

	connAllocTimeout := 2 * time.Second
	config := pgx.ConnPoolConfig{
		ConnConfig:     *defaultConnConfig,
		MaxConnections: 1,
		AcquireTimeout: connAllocTimeout,
	}

	pool, err := pgx.NewRsConnPool(config)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	// Consume all connections ...
	allConnections := acquireAllRsConnPoolConnections(t, pool, config.MaxConnections)
	defer releaseAllRsConnPoolConnections(pool, allConnections)

	// ... then try to consume 1 more. It should fail after a short timeout.
	_, timeTaken, err := acquireRsConnPoolWithTimeTaken(pool)

	if err == nil || err.Error() != "Timeout: All connections in pool are busy" {
		t.Fatalf("Expected error to be 'Timeout: All connections in pool are busy', instead it was '%v'", err)
	}
	if timeTaken < connAllocTimeout {
		t.Fatalf("Expected connection allocation time to be at least %v, instead it was '%v'", connAllocTimeout, timeTaken)
	}
}

func TestRsConnPoolWithoutAcquireTimeoutSet(t *testing.T) {
	t.Parallel()

	maxConnections := 1
	pool := createRsConnPool(t, maxConnections)
	defer pool.Close()

	// Consume all connections ...
	allConnections := acquireAllRsConnPoolConnections(t, pool, maxConnections)

	// ... then try to consume 1 more. It should hang forever.
	// To unblock it we release the previously taken connection in a goroutine.
	stopDeadWaitTimeout := 5 * time.Second
	timer := time.AfterFunc(stopDeadWaitTimeout, func() {
		releaseAllRsConnPoolConnections(pool, allConnections)
	})
	defer timer.Stop()

	conn, timeTaken, err := acquireRsConnPoolWithTimeTaken(pool)
	if err == nil {
		pool.Release(conn)
	} else {
		t.Fatalf("Expected error to be nil, instead it was '%v'", err)
	}
	if timeTaken < stopDeadWaitTimeout {
		t.Fatalf("Expected connection allocation time to be at least %v, instead it was '%v'", stopDeadWaitTimeout, timeTaken)
	}
}

func TestRsConnPoolReleaseWithTransactions(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 2)
	defer pool.Close()

	conn, err := pool.Acquire()
	if err != nil {
		t.Fatalf("Unable to acquire connection: %v", err)
	}
	mustExec(t, conn, "begin")
	if _, err = conn.Exec("selct"); err == nil {
		t.Fatal("Did not receive expected error")
	}

	if conn.TxStatus != 'E' {
		t.Fatalf("Expected TxStatus to be 'E', instead it was '%c'", conn.TxStatus)
	}

	pool.Release(conn)

	if conn.TxStatus != 'I' {
		t.Fatalf("Expected release to rollback errored transaction, but it did not: '%c'", conn.TxStatus)
	}

	conn, err = pool.Acquire()
	if err != nil {
		t.Fatalf("Unable to acquire connection: %v", err)
	}
	mustExec(t, conn, "begin")
	if conn.TxStatus != 'T' {
		t.Fatalf("Expected txStatus to be 'T', instead it was '%c'", conn.TxStatus)
	}

	pool.Release(conn)

	if conn.TxStatus != 'I' {
		t.Fatalf("Expected release to rollback uncommitted transaction, but it did not: '%c'", conn.TxStatus)
	}
}

func TestRsConnPoolAcquireAndReleaseCycleAutoConnect(t *testing.T) {
	t.Parallel()

	maxConnections := 3
	pool := createRsConnPool(t, maxConnections)
	defer pool.Close()

	doSomething := func() {
		c, err := pool.Acquire()
		if err != nil {
			t.Fatalf("Unable to Acquire: %v", err)
		}
		rows, _ := c.Query("select 1")
		rows.Close()
		pool.Release(c)
	}

	for i := 0; i < 1000; i++ {
		doSomething()
	}

	stat := pool.Stat()
	if stat.CurrentConnections != 1 {
		t.Fatalf("Pool shouldn't have established more connections when no contention: %v", stat.CurrentConnections)
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doSomething()
		}()
	}
	wg.Wait()

	stat = pool.Stat()
	if stat.CurrentConnections != stat.MaxConnections {
		t.Fatalf("Pool should have used all possible connections: %v", stat.CurrentConnections)
	}
}

func TestRsConnPoolReleaseDiscardsDeadConnections(t *testing.T) {
	t.Parallel()

	// Run timing sensitive test many times
	for i := 0; i < 50; i++ {
		func() {
			maxConnections := 3
			pool := createRsConnPool(t, maxConnections)
			defer pool.Close()

			var c1, c2 *pgx.Conn
			var err error
			var stat pgx.ConnPoolStat

			if c1, err = pool.Acquire(); err != nil {
				t.Fatalf("Unexpected error acquiring connection: %v", err)
			}
			defer func() {
				if c1 != nil {
					pool.Release(c1)
				}
			}()

			if c2, err = pool.Acquire(); err != nil {
				t.Fatalf("Unexpected error acquiring connection: %v", err)
			}
			defer func() {
				if c2 != nil {
					pool.Release(c2)
				}
			}()

			if _, err = c2.Exec("select pg_terminate_backend($1)", c1.Pid); err != nil {
				t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
			}

			// do something with the connection so it knows it's dead
			rows, _ := c1.Query("select 1")
			rows.Close()
			if rows.Err() == nil {
				t.Fatal("Expected error but none occurred")
			}

			if c1.IsAlive() {
				t.Fatal("Expected connection to be dead but it wasn't")
			}

			stat = pool.Stat()
			if stat.CurrentConnections != 2 {
				t.Fatalf("Unexpected CurrentConnections: %v", stat.CurrentConnections)
			}
			if stat.AvailableConnections != 0 {
				t.Fatalf("Unexpected AvailableConnections: %v", stat.CurrentConnections)
			}

			pool.Release(c1)
			c1 = nil // so it doesn't get released again by the defer

			stat = pool.Stat()
			if stat.CurrentConnections != 1 {
				t.Fatalf("Unexpected CurrentConnections: %v", stat.CurrentConnections)
			}
			if stat.AvailableConnections != 0 {
				t.Fatalf("Unexpected AvailableConnections: %v", stat.CurrentConnections)
			}
		}()
	}
}

func TestRsConnPoolReset(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 5)
	defer pool.Close()

	inProgressRows := []*pgx.Rows{}

	// Start some queries and reset pool while they are in progress
	for i := 0; i < 10; i++ {
		rows, err := pool.Query("select generate_series(1,5)::bigint")
		if err != nil {
			t.Fatal(err)
		}

		inProgressRows = append(inProgressRows, rows)
		pool.Reset()
	}

	// Check that the queries are completed
	for _, rows := range inProgressRows {
		var expectedN int64

		for rows.Next() {
			expectedN++
			var n int64
			err := rows.Scan(&n)
			if err != nil {
				t.Fatal(err)
			}
			if expectedN != n {
				t.Fatalf("Expected n to be %d, but it was %d", expectedN, n)
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}

	// pool should be in fresh state due to previous reset
	stats := pool.Stat()
	if stats.CurrentConnections != 0 || stats.AvailableConnections != 0 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}
}

func TestRsConnPoolTransaction(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 2)
	defer pool.Close()

	stats := pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}

	tx, err := pool.Begin()
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	defer tx.Rollback()

	var n int32
	err = tx.QueryRow("select 40+$1", 2).Scan(&n)
	if err != nil {
		t.Fatalf("tx.QueryRow Scan failed: %v", err)
	}
	if n != 42 {
		t.Errorf("Expected 42, got %d", n)
	}

	stats = pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 0 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("tx.Rollback failed: %v", err)
	}

	stats = pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}
}

func TestRsConnPoolTransactionIso(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 2)
	defer pool.Close()

	tx, err := pool.BeginIso(pgx.Serializable)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	defer tx.Rollback()

	var level string
	err = tx.QueryRow("select current_setting('transaction_isolation')").Scan(&level)
	if err != nil {
		t.Fatalf("tx.QueryRow failed: %v", level)
	}

	if level != "serializable" {
		t.Errorf("Expected to be in isolation level %v but was %v", "serializable", level)
	}
}

func TestRsConnPoolBeginRetry(t *testing.T) {
	t.Parallel()

	// Run timing sensitive test many times
	for i := 0; i < 50; i++ {
		func() {
			pool := createRsConnPool(t, 2)
			defer pool.Close()

			killerConn, err := pool.Acquire()
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Release(killerConn)

			victimConn, err := pool.Acquire()
			if err != nil {
				t.Fatal(err)
			}
			pool.Release(victimConn)

			// Terminate connection that was released to pool
			if _, err = killerConn.Exec("select pg_terminate_backend($1)", victimConn.Pid); err != nil {
				t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
			}

			// Since victimConn is the only available connection in the pool, pool.Begin should
			// try to use it, fail, and allocate another connection
			tx, err := pool.Begin()
			if err != nil {
				t.Fatalf("pool.Begin failed: %v", err)
			}
			defer tx.Rollback()

			var txPid int32
			err = tx.QueryRow("select pg_backend_pid()").Scan(&txPid)
			if err != nil {
				t.Fatalf("tx.QueryRow Scan failed: %v", err)
			}
			if txPid == victimConn.Pid {
				t.Error("Expected txPid to defer from killed conn pid, but it didn't")
			}
		}()
	}
}

func TestRsConnPoolQuery(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 2)
	defer pool.Close()

	var sum, rowCount int32

	rows, err := pool.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("pool.Query failed: %v", err)
	}

	stats := pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 0 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}

	stats = pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}
}

func TestRsConnPoolQueryConcurrentLoad(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 10)
	defer pool.Close()

	n := 100
	done := make(chan bool)

	for i := 0; i < n; i++ {
		go func() {
			defer func() { done <- true }()
			var rowCount int32

			rows, err := pool.Query("select generate_series(1,$1)", 1000)
			if err != nil {
				t.Fatalf("pool.Query failed: %v", err)
			}
			defer rows.Close()

			for rows.Next() {
				var n int32
				err = rows.Scan(&n)
				if err != nil {
					t.Fatalf("rows.Scan failed: %v", err)
				}
				if n != rowCount+1 {
					t.Fatalf("Expected n to be %d, but it was %d", rowCount+1, n)
				}
				rowCount++
			}

			if rows.Err() != nil {
				t.Fatalf("conn.Query failed: %v", rows.Err())
			}

			if rowCount != 1000 {
				t.Error("Select called onDataRow wrong number of times")
			}

			_, err = pool.Exec("--;")
			if err != nil {
				t.Fatalf("pool.Exec failed: %v", err)
			}
		}()
	}

	for i := 0; i < n; i++ {
		<-done
	}
}

func TestRsConnPoolQueryRow(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 2)
	defer pool.Close()

	var n int32
	err := pool.QueryRow("select 40+$1", 2).Scan(&n)
	if err != nil {
		t.Fatalf("pool.QueryRow Scan failed: %v", err)
	}

	if n != 42 {
		t.Errorf("Expected 42, got %d", n)
	}

	stats := pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}
}

func TestRsConnPoolExec(t *testing.T) {
	t.Parallel()

	pool := createRsConnPool(t, 2)
	defer pool.Close()

	results, err := pool.Exec("create temporary table foo(id integer primary key);")
	if err != nil {
		t.Fatalf("Unexpected error from pool.Exec: %v", err)
	}
	if results != "CREATE TABLE" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}

	results, err = pool.Exec("insert into foo(id) values($1)", 1)
	if err != nil {
		t.Fatalf("Unexpected error from pool.Exec: %v", err)
	}
	if results != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}

	results, err = pool.Exec("drop table foo;")
	if err != nil {
		t.Fatalf("Unexpected error from pool.Exec: %v", err)
	}
	if results != "DROP TABLE" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}
