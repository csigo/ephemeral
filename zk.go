package ephemeral

/*zk.go is the zookeeper implementation of ephemeral.*/

import (
	"errors"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/facebookgo/stats"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"
)

const (
	// Always set a fixed version number into zk since we're not using version.
	version = -1
	// dirValue is string to indicate node is a dir
	dirValue = "__zookeeper_dir__"
)

// NewZKEphemeral creates a zk implementation of ephemeral
// TODO: add option to take listener.
func NewZKEphemeral(servers []string, options ...func(*ZKEphemeral) error) (*ZKEphemeral, error) {
	impl := &ZKEphemeral{
		recvTimeout: time.Second,
		ctr:         dummyStat{},
	}

	// Set options.
	for _, option := range options {
		if err := option(impl); err != nil {
			return nil, err
		}
	}

	// Connect to zk and we're done.
	conn, _, err := zk.Connect(servers, impl.recvTimeout)
	if err != nil {
		return nil, err
	}
	impl.conn = conn

	return impl, nil
}

// ZKRecvTimeout sets timeout when receiving response from zookeeper server.
func ZKRecvTimeout(timeout time.Duration) func(*ZKEphemeral) error {
	return func(z *ZKEphemeral) error {
		z.recvTimeout = timeout
		return nil
	}
}

// ZKCounter sets counter client.
func ZKCounter(counter stats.Client) func(*ZKEphemeral) error {
	return func(z *ZKEphemeral) error {
		if counter == nil {
			return errors.New("nil counter is not allowed")
		}
		z.ctr = counter
		return nil
	}
}

// ZKEphemeral is the zk implementation for ephemeral interface.
type ZKEphemeral struct {
	recvTimeout time.Duration
	conn        *zk.Conn
	ctr         stats.Client
}

// Status returns connection status toward zookeeper.
func (z *ZKEphemeral) Status() zk.State {
	defer z.ctr.BumpTime("ephemeral.zk.status.proc_time").End()
	if z.conn == nil {
		return zk.StateUnknown
	}
	return z.conn.State()
}

// AddDir adds a permenant directory to hold keys for zk implementation.
func (z *ZKEphemeral) AddDir(ctx context.Context, path string) error {
	defer z.ctr.BumpTime("ephemeral.zk.adddir.proc_time").End()
	// write dirValue to indicate the node represent a dir
	_, err := z.conn.Create(path, []byte(dirValue), int32(0), zk.WorldACL(zk.PermAll))
	return translate(err)
}

// AddKey add an ephemeral key for zk implementation.
func (z *ZKEphemeral) AddKey(ctx context.Context, path, value string) error {
	defer z.ctr.BumpTime("ephemeral.zk.addkey.proc_time").End()
	// check value is not equal to dirValue
	if value == dirValue {
		return fmt.Errorf("key value can't be reserved word<%s>", dirValue)
	}

	// create node
	_, err := z.conn.Create(path, []byte(value), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	// TODO: When zk connection is lost, samuel library trys to reconnect automatically.
	// However the existing zk node may disappear because zk believe the ephemeral node is
	// lost. Need to listen to has-session event and re-build the node.
	return translate(err)
}

// List lists children of a node.
func (z *ZKEphemeral) List(ctx context.Context, path string, watch bool) <-chan *ListResponse {
	defer z.ctr.BumpTime("ephemeral.zk.list.proc_time").End()
	receiver := make(chan *ListResponse, 1)
	// If watch is false, call Children() directly.
	if !watch {
		z.ctr.BumpSum("ephemeral.zk.list.no_watch", 1)
		n, _, err := z.conn.Children(path)
		receiver <- &ListResponse{Children: n, Err: translate(err)}
		close(receiver)
		return receiver
	}

	// Otherwise, fork a goroutine to call ChildrenW() and watch the changes.
	z.ctr.BumpSum("ephemeral.zk.list.watch", 1)
	get2Receiver := func(path string, stop bool) (<-chan zk.Event, error) {
		if stop {
			receiver <- &ListResponse{Err: ErrWatchStoppedByUser}
			return nil, ErrWatchStoppedByUser
		}
		children, _, event, err := z.conn.ChildrenW(path)
		receiver <- &ListResponse{Children: children, Err: translate(err)}
		return event, err
	}

	// Fork a go routine to watch changes of children
	go func() {
		z.loop(path, ctx.Done(), get2Receiver)
		close(receiver)
	}()

	return receiver
}

// Get returns value stored in an ephemeral node for zk implementation.
func (z *ZKEphemeral) Get(ctx context.Context, path string) (string, error) {
	// get node value
	b, _, err := z.conn.Get(path)
	if err != nil {
		return "", translate(err)
	}
	// if b == dirValue return error
	if string(b) == dirValue {
		return "", fmt.Errorf("path %v is a directory", path)
	}
	return string(b), nil
}

// Close closes the zk connection
func (z *ZKEphemeral) Close() error {
	defer z.ctr.BumpTime("ephemeral.zk.close.proc_time").End()
	z.conn.Close()
	return nil
}

func (z *ZKEphemeral) loop(
	path string,
	done <-chan struct{},
	get2Receiver func(path string, stop bool) (<-chan zk.Event, error),
) {
	for {
		event, err := get2Receiver(path, false)
		z.ctr.BumpSum("ephemeral.zk.loop.get2receiver", 1)
		if err != nil {
			log.Errorf("[zk] got err resp on <%s>:%s", path, err)
			z.ctr.BumpSum("ephemeral.zk.loop.get2receiver.err", 1)
			return
		}
		// Wait for the next event or cancellation.
		select {
		case <-done:
			z.ctr.BumpSum("ephemeral.zk.loop.done", 1)
			log.Infof("Received done signal. Quitting.")
			_, _ = get2Receiver("", true)
			log.Infof("Writes channel stop by user error.")
		case ev := <-event:
			z.ctr.BumpSum("ephemeral.zk.loop.receive", 1)
			log.Infof("Receive event %v", ev)
			// Go to the next loop to get children again
			continue
		}
	}
}

func translate(err error) error {
	switch err {
	case zk.ErrNoNode:
		return ErrPathNotFound
	case zk.ErrNodeExists:
		return ErrPathExists
	default:
		return err
	}
}
