package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// driver is a system under test. It opens per-client connections and seeds the
// keyspace; the actual ops live on conn so per-client state (an etcd lease
// session, a cask lock session) has somewhere to live.
type driver interface {
	open(id int) (conn, error)
	seed(ctx context.Context, keys []string, val []byte) error
	close() error
}

// conn is one client's handle to the system under test.
type conn interface {
	write(ctx context.Context, key string, val []byte) error
	read(ctx context.Context, key string) (found bool, err error)
	cas(ctx context.Context, key string, old, val []byte) (applied bool, err error)
	lockCycle(ctx context.Context, name string) error
	close() error
}

func newDriver(cfg config) (driver, error) {
	eps := splitEndpoints(cfg.endpoints)
	switch cfg.target {
	case "cask":
		if len(eps) == 0 {
			eps = []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"}
		}
		return newCaskDriver(eps, cfg.clients), nil
	case "etcd":
		if len(eps) == 0 {
			eps = []string{"127.0.0.1:2379", "127.0.0.1:2380", "127.0.0.1:2381"}
		}
		return newEtcdDriver(eps)
	default:
		return nil, fmt.Errorf("unknown target %q (want cask|etcd)", cfg.target)
	}
}

func splitEndpoints(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ---- cask driver (HTTP client API of cmd/cask) ----

type caskDriver struct {
	bases []string
	hc    *http.Client
}

func newCaskDriver(bases []string, clients int) *caskDriver {
	// One shared client with a pool deep enough that every goroutine keeps a
	// warm keep-alive connection to its endpoint — otherwise we'd measure TCP
	// setup, not cask.
	perHost := clients/len(bases) + 2
	return &caskDriver{
		bases: bases,
		hc: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        clients + len(bases),
				MaxIdleConnsPerHost: perHost,
				MaxConnsPerHost:     perHost,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (d *caskDriver) open(id int) (conn, error) {
	// Spread clients across the three nodes round-robin.
	return &caskConn{base: d.bases[id%len(d.bases)], hc: d.hc, session: fmt.Sprintf("bench-%d", id)}, nil
}

func (d *caskDriver) seed(ctx context.Context, keys []string, val []byte) error {
	c := &caskConn{base: d.bases[0], hc: d.hc}
	for _, k := range keys {
		if err := c.write(ctx, k, val); err != nil {
			return err
		}
	}
	return nil
}

func (d *caskDriver) close() error { d.hc.CloseIdleConnections(); return nil }

type caskConn struct {
	base       string
	hc         *http.Client
	session    string
	sessionSet bool
}

func (c *caskConn) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body) // drain so the connection is reused
	return resp.StatusCode, out, nil
}

func (c *caskConn) write(ctx context.Context, key string, val []byte) error {
	code, body, err := c.do(ctx, http.MethodPut, "/kv/"+key, val)
	if err != nil {
		return err
	}
	if code != http.StatusNoContent {
		return fmt.Errorf("put %s: %d %s", key, code, body)
	}
	return nil
}

func (c *caskConn) read(ctx context.Context, key string) (bool, error) {
	code, _, err := c.do(ctx, http.MethodGet, "/kv/"+key, nil)
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("get %s: %d", key, code)
	}
}

func (c *caskConn) cas(ctx context.Context, key string, old, val []byte) (bool, error) {
	code, _, err := c.do(ctx, http.MethodPost, "/cas/"+key+"?expect="+url.QueryEscape(string(old)), val)
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusNoContent:
		return true, nil
	case http.StatusConflict:
		return false, nil
	default:
		return false, fmt.Errorf("cas %s: %d", key, code)
	}
}

func (c *caskConn) lockCycle(ctx context.Context, name string) error {
	if !c.sessionSet {
		if code, body, err := c.do(ctx, http.MethodPost, "/session/"+c.session+"?ttl=30", nil); err != nil {
			return err
		} else if code != http.StatusNoContent {
			return fmt.Errorf("session grant: %d %s", code, body)
		}
		c.sessionSet = true
	}
	q := "?session=" + url.QueryEscape(c.session)
	if code, body, err := c.do(ctx, http.MethodPost, "/lock/"+name+q, nil); err != nil {
		return err
	} else if code != http.StatusOK {
		return fmt.Errorf("acquire %s: %d %s", name, code, body)
	}
	if code, body, err := c.do(ctx, http.MethodDelete, "/lock/"+name+q, nil); err != nil {
		return err
	} else if code != http.StatusNoContent {
		return fmt.Errorf("release %s: %d %s", name, code, body)
	}
	return nil
}

func (c *caskConn) close() error { return nil }

// ---- etcd driver (clientv3) ----

type etcdDriver struct {
	cli *clientv3.Client
}

func newEtcdDriver(endpoints []string) (*etcdDriver, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &etcdDriver{cli: cli}, nil
}

func (d *etcdDriver) open(int) (conn, error) { return &etcdConn{cli: d.cli}, nil }

func (d *etcdDriver) seed(ctx context.Context, keys []string, val []byte) error {
	for _, k := range keys {
		if _, err := d.cli.Put(ctx, k, string(val)); err != nil {
			return err
		}
	}
	return nil
}

func (d *etcdDriver) close() error { return d.cli.Close() }

type etcdConn struct {
	cli  *clientv3.Client
	sess *concurrency.Session // lazily created for the lock workload
}

func (c *etcdConn) write(ctx context.Context, key string, val []byte) error {
	_, err := c.cli.Put(ctx, key, string(val))
	return err
}

func (c *etcdConn) read(ctx context.Context, key string) (bool, error) {
	// Default (no WithSerializable) is a linearizable quorum read — the fair
	// counterpart to cask's linearizable Get.
	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) > 0, nil
}

func (c *etcdConn) cas(ctx context.Context, key string, old, val []byte) (bool, error) {
	// old == nil means "key must be absent" (conditional create), which in etcd
	// is a CreateRevision==0 guard; otherwise compare the current value. This
	// mirrors cask's CAS, where an empty expect matches an absent key.
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	if len(old) > 0 {
		cmp = clientv3.Compare(clientv3.Value(key), "=", string(old))
	}
	resp, err := c.cli.Txn(ctx).
		If(cmp).
		Then(clientv3.OpPut(key, string(val))).
		Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func (c *etcdConn) lockCycle(ctx context.Context, name string) error {
	if c.sess == nil {
		s, err := concurrency.NewSession(c.cli, concurrency.WithTTL(30))
		if err != nil {
			return err
		}
		c.sess = s
	}
	m := concurrency.NewMutex(c.sess, "bench/lock/"+name)
	if err := m.Lock(ctx); err != nil {
		return err
	}
	return m.Unlock(ctx)
}

func (c *etcdConn) close() error {
	if c.sess != nil {
		return c.sess.Close()
	}
	return nil
}
