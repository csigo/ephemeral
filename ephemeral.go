// Package ephemeral defines mechnism to manage ephemeral nodes.
//
//  func main() {
//    // Creates a ephemeral implementation with ttl equals to 50 millisecond.
//    ep := NewEtcdEphemeral(50 * time.Millisecond)
//    ctx := context.Background()
//    ep.AddDir(ctx, /root)
//    ep.AddKey(ctx, /root/key1)
//  }
//
//  func anotherServer() {
//    ch := ep.List(ctx, /root, true)
//    for resp := range(ch) {
//      // Channel will return []string{"key1"}, and []string{} when key1 is disconnected
//    }
//  }
package ephemeral

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/net/context"
)

const (
	pkgName = "ephemeral"
)

// command related errors
var (
	ErrPathNotFound       = errors.New("Path not found")
	ErrPathExists         = errors.New("Path already exists")
	ErrWatchStoppedByUser = errors.New("Watch stopped by the user")
)

// ListResponse holds the response of list request.
type ListResponse struct {
	Children []string
	Err      error
}

// Ephemeral keeps track of directories of ephemeral nodes.
type Ephemeral interface {
	// AddDir adds a permenant directory to hold keys.
	AddDir(ctx context.Context, path string) error

	// Add adds an ephemeral key with value. The key is bound to the creator client and will be
	// deleted when the client disconnects.
	AddKey(ctx context.Context, path, value string) error

	// List lists children of a node. If watch is true, List returns channel with current
	// children and future changes until error or cancellation. Otherwise List returns an
	// one-element channel containing the children.
	List(ctx context.Context, path string, watch bool) <-chan *ListResponse

	// Get gets value of a key. Get does not apply to a directory.
	Get(ctx context.Context, path string) (string, error)

	// Close closes connection.
	Close() error
}

// EnsurePath ensures that the path like /root/node1 exists in storer.
func EnsurePath(em Ephemeral, path string) error {
	// if path is empty, just return
	if path == "" {
		return nil
	}
	//recursively create root, disregard error
	p := ""
	for _, r := range strings.Split(path[1:], "/") {
		p = fmt.Sprintf("%s/%s", p, r)
		err := em.AddDir(context.Background(), p)
		if err != nil && err != ErrPathExists {
			return err
		}
	}
	return nil
}
