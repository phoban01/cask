// Package nebula provides a Nebula-overlay implementation of
// [transport.Network], so cask's ConnectRPC consensus transport can ride an
// encrypted, NAT-traversing mesh across clouds instead of the host TCP stack.
//
// It is optional: the binary selects it only when a Nebula config is supplied,
// otherwise it uses plain TCP. Because the overlay runs in userspace (gvisor
// netstack) there is no TUN device and no root requirement — cask stays a single
// embeddable binary, which is the point of choosing Nebula for the multi-cloud
// story. Every inter-node connection is mutually authenticated and encrypted by
// Nebula and addressed by a stable overlay IP, so a node reaches its peers
// identically regardless of which cloud or NAT it sits behind.
package nebula

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"

	"github.com/sirupsen/logrus"
	nebula "github.com/slackhq/nebula"
	"github.com/slackhq/nebula/config"
	"github.com/slackhq/nebula/overlay"
	"github.com/slackhq/nebula/service"

	"github.com/phoban01/cask/internal/transport"
)

// Network is a [transport.Network] backed by a Nebula userspace overlay.
type Network struct {
	svc *service.Service
}

// Compile-time check that the Nebula backend satisfies the transport seam.
var _ transport.Network = (*Network)(nil)

// New builds a Nebula-backed Network from a standard Nebula YAML config (pki
// cert/key/ca, lighthouse hosts, and so on). It runs entirely in userspace, so
// no TUN device or root is needed. The returned Network's listener and HTTP
// client both ride the overlay; cask's ConnectRPC server and clients are built
// from them unchanged.
//
// Nebula's API requires a *logrus.Logger; rather than leak that into cask, this
// takes a *slog.Logger and bridges Nebula's log output onto it. logrus is thus
// confined entirely to this file.
func New(yamlConfig string, logger *slog.Logger) (*Network, error) {
	if logger == nil {
		logger = slog.Default()
	}
	var c config.C
	if err := c.LoadString(yamlConfig); err != nil {
		return nil, fmt.Errorf("nebula: load config: %w", err)
	}
	// configTest=false: build a live control. NewUserDeviceFromConfig selects
	// the userspace (netstack) device rather than a kernel TUN.
	control, err := nebula.Main(&c, false, "cask", logrusBridge(logger), overlay.NewUserDeviceFromConfig)
	if err != nil {
		return nil, fmt.Errorf("nebula: init: %w", err)
	}
	// service.New calls control.Start() (handshakes + lighthouse registration)
	// and stands up the userspace TCP/IP stack over the tunnel.
	svc, err := service.New(control)
	if err != nil {
		return nil, fmt.Errorf("nebula: service: %w", err)
	}
	return &Network{svc: svc}, nil
}

// Listen serves on the overlay address (e.g. ":8001", bound to this node's
// overlay IP).
func (n *Network) Listen(ctx context.Context, address string) (net.Listener, error) {
	return n.svc.Listen("tcp", address)
}

// HTTPClient returns a client whose connections are dialed over the overlay.
// This is the single object cask's ConnectRPC AcceptorClients are constructed
// from — the consensus transport is otherwise unaware Nebula is involved.
func (n *Network) HTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DialContext: n.svc.DialContext},
	}
}

// Close tears down the overlay and all tunnels.
func (n *Network) Close() error { return n.svc.Close() }

// logrusBridge returns a *logrus.Logger that emits nothing of its own and
// instead forwards every entry to log, mapping levels and fields across. This
// is the only logrus that exists in cask, and it never escapes this package.
func logrusBridge(log *slog.Logger) *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)       // the hook does all the work
	l.SetLevel(logrus.DebugLevel) // let every entry reach the hook; slog filters
	l.AddHook(&slogHook{log: log})
	return l
}

type slogHook struct{ log *slog.Logger }

func (h *slogHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *slogHook) Fire(e *logrus.Entry) error {
	ctx := e.Context
	if ctx == nil {
		ctx = context.Background()
	}
	attrs := make([]any, 0, len(e.Data)*2)
	for k, v := range e.Data {
		attrs = append(attrs, k, v)
	}
	h.log.Log(ctx, slogLevel(e.Level), e.Message, attrs...)
	return nil
}

func slogLevel(l logrus.Level) slog.Level {
	switch l {
	case logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel:
		return slog.LevelError
	case logrus.WarnLevel:
		return slog.LevelWarn
	case logrus.InfoLevel:
		return slog.LevelInfo
	default: // Debug, Trace
		return slog.LevelDebug
	}
}
