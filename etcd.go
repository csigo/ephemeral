package ephemeral

/*
etcd.go implementes the ephemeral node based on etcd. it uses etcd TTL to simulate ephemeral nodes.
with TTL, the node will be removed automatically when client keeps down over TTL, which is like session's behavior.
in brief, we keep updating the node with TTL, and the node will exists until the node is down.
however, there will be lots of event for updating node's TTL. this problem incurs large overhead of list.
we implement list with watching by etcd watch. lots of TTL events make etcd watch send lots of events to "all list."
if there is N nodes and all of them register/list on the same path, etcd watch has to send N^2 events to them.
consequently, this cause high cpu usage of etcd. to mitigate this problem, we create a shadow path in etcd. when client add node and list
on "/path", we create "/watcherDir/path"(no TTL event) for watching, and maintain only 1 master to monitor "/path"(has TTL event) in cluster.
except of TTL event, if any other event happens on "/path", the master monitor will apply the event on "/watcherDir/path."
then, lists can receive the events excluding TTL events. this solution can guarantee the etcd watch has to notify TTL event to only 1 machine.
besides, when list watching, there may be transient error(node exists, but actually doesn't), but we only care the list result eventually.
this implemetation ensures the result of list watching is right.
*/

import (
	"errors"
	"fmt"
	p "path"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	etcdErr "github.com/coreos/etcd/error"
	"github.com/coreos/go-etcd/etcd"
	etcdlock "github.com/datawisesystems/etcd-lock"
	"github.com/facebookgo/stats"
	"golang.org/x/net/context"
)

const (
	// DefaultEphemeralTTL is the default TTL for ephemeral etcd implementation(in second).
	DefaultEphemeralTTL = uint64(2)
	// heartbeat retry times
	heartbeatRetry = 3
	// watcherDir is the path which watcher is watching. notice that it stores only watching path, not real data.
	WatcherDir = "/_watcherDir"
	// watcherLock is to acquire the permission to be master goroutine to maintain watch.
	watcherLock = "_watcher.lock"
	// watchRetryDuration is the duration to retry watch again after 401
	watchRetryDuration = 2 * time.Second
	// listMasterLockTTL is the ttl of the list watch master maintainer
	listMasterLockTTL = uint64(10)
	// doHandlerRetry is the retry time for dodiff to retry
	doHandlerRetry = 3
)

var (
	watchBufferSize = 1000
	infoFunc        = log.Infof
)

// NewEtcdEphemeral creates a etcd implementation of ephemeral
// TODO: add option for user to support:
//       1. fatal after retrying set heartbeat many times
func NewEtcdEphemeral(client *etcd.Client, options ...func(*EtcdEphemeral) error) (*EtcdEphemeral, error) {
	impl := &EtcdEphemeral{
		client:       client,
		ctr:          dummyStat{},
		ephemeralTTL: DefaultEphemeralTTL,
		listCancel:   make(map[context.Context]func()),
	}

	// Set options.
	for _, option := range options {
		if err := option(impl); err != nil {
			return nil, err
		}
	}

	// ensure ephemeral dir exists, the error is not important
	if _, err := client.CreateDir(WatcherDir, 0); err != nil &&
		translateEtcdErr(err) != ErrPathExists {
		return nil, err
	}

	return impl, nil
}

// EtcdEphemeralTTL sets ephemeral ttl to client
func EtcdEphemeralTTL(ttl uint64) func(*EtcdEphemeral) error {
	return func(e *EtcdEphemeral) error {
		e.ephemeralTTL = ttl
		return nil
	}
}

// EtcdCounter sets counter client.
func EtcdCounter(counter stats.Client) func(*EtcdEphemeral) error {
	return func(e *EtcdEphemeral) error {
		if counter == nil {
			return errors.New("nil counter is not allowed")
		}
		e.ctr = counter
		return nil
	}
}

// EtcdEphemeral is the etcd implementation for ephemeral interface.
type EtcdEphemeral struct {
	client       *etcd.Client
	ctr          stats.Client
	ephemeralTTL uint64

	cancelLock sync.Mutex
	listCancel map[context.Context]func()
}

// AddDir adds a permenant directory to hold keys for etcd implementation.
func (e *EtcdEphemeral) AddDir(ctx context.Context, path string) error {
	defer e.ctr.BumpTime("ephemeral.etcd.adddir.proc_time").End()
	defer e.ctr.BumpSum("ephemeral.etcd.adddir.done", 1)
	// ttl is set 0 for permenant directory
	// if the path under watchDir has existed, it will return ErrPathExists,
	// and we don't care about it(just a shadow dir)
	// if not, there must be error when it is created, so return error.
	if _, err := e.client.CreateDir(ephemeralPath(path), 0); err != nil &&
		translateEtcdErr(err) != ErrPathExists {
		return translateEtcdErr(err)
	}
	// not like above CreateDir, we care about the real path when creating the dir.
	// so, it returns all error, even though the dir has existed.
	if _, err := e.client.CreateDir(path, 0); err != nil {
		return translateEtcdErr(err)
	}
	return nil
}

// AddKey add an ephemeral key for etcd implementation.
func (e *EtcdEphemeral) AddKey(ctx context.Context, path, value string) error {
	defer e.ctr.BumpTime("ephemeral.etcd.addkey.proc_time").End()
	defer e.ctr.BumpSum("ephemeral.etcd.adddkey.done", 1)
	if _, err := e.client.Create(path, value, e.ephemeralTTL); err != nil {
		// if failed, try create again, because it may be caused by restarting without deleting node.
		log.Warningf("[etcd] Create key failed first time, err: %v. Try 2nd time.", err)
		// First, sleep for 2*TTL to ensure previos node vanishing.
		time.Sleep(2 * time.Duration(e.ephemeralTTL) * time.Second)
		// Create again. if it still failed, return error.
		if _, err := e.client.Create(path, value, e.ephemeralTTL); err != nil {
			log.Errorf("[etcd] Create key failed. Err: %v", err)
			return translateEtcdErr(err)
		}
	}
	heartbeat := func() error {
		defer e.ctr.BumpSum("ephemeral.etcd.heartbeat.done", 1)
		var err error
		for i := heartbeatRetry; i >= 0; i-- {
			_, err = e.client.Set(path, value, e.ephemeralTTL)
			if err == nil {
				return nil
			}
		}
		return err
	}
	deleteNow := func() error {
		_, err := e.client.Delete(path, false)
		if err != nil {
			log.Errorf("Delete path failed, err:%v", err)
		}
		return err
	}
	innerCtx, cancel := context.WithCancel(ctx)
	e.cancelLock.Lock()
	e.listCancel[innerCtx] = cancel
	e.cancelLock.Unlock()
	// send heartbeat periodically (0.4*TTL). any number below 0.5 should be works here
	// because it allows we to lose at most two heartbeat.
	ticker := time.NewTicker(time.Duration(e.ephemeralTTL) * time.Second * 4 / 10)
	go func() {
		for {
			select {
			case <-innerCtx.Done():
				ticker.Stop()
				deleteNow()
				e.cancelLock.Lock()
				delete(e.listCancel, innerCtx)
				e.cancelLock.Unlock()
				return
			case <-ticker.C:
				if err := heartbeat(); err != nil {
					// TODO: need another mechanism to export errors
					e.ctr.BumpSum("ephemeral.heartbeat.err", 1)
					log.Errorf("fail to set heartbeat for %s, err:%v", path, err)
				}
			}
		}
	}()
	return nil
}

// List lists children of a node.
func (e *EtcdEphemeral) List(ctx context.Context, path string, watch bool) <-chan *ListResponse {
	defer e.ctr.BumpTime("ephemeral.etcd.list.proc_time").End()
	receiver := make(chan *ListResponse, watchBufferSize)
	if !watch {
		e.ctr.BumpSum("ephemeral.etcd.list.start", 1)
		resp, err := e.client.Get(path, true, false)
		children, _ := toChildren(path, resp)
		receiver <- &ListResponse{Children: children, Err: translateEtcdErr(err)}
		close(receiver)
		e.ctr.BumpSum("ephemeral.etcd.list.done", 1)
		return receiver
	}
	e.ctr.BumpSum("ephemeral.etcd.listwatch.start", 1)
	e.etcdLoop(ctx, path, receiver)
	return receiver
}

// Get returns value stored in an ephemeral node for etcd implementation.
func (e *EtcdEphemeral) Get(ctx context.Context, path string) (string, error) {
	defer e.ctr.BumpTime("ephemeral.etcd.get.proc_time").End()
	defer e.ctr.BumpSum("ephemeral.etcd.get.done", 1)
	v, err := e.client.Get(path, true, false)
	if err != nil {
		return "", translateEtcdErr(err)
	}
	// if path is dir, return error
	if v.Node.Dir {
		return "", fmt.Errorf("path %v is a directory", path)
	}
	return v.Node.Value, nil
}

// Close closes the etcd connection
func (e *EtcdEphemeral) Close() error {
	defer e.ctr.BumpTime("ephemeral.etcd.close.proc_time").End()
	defer e.ctr.BumpSum("ephemeral.etcd.close.done", 1)
	for _, cancel := range e.listCancel {
		cancel()
	}
	return nil
}

func (e *EtcdEphemeral) etcdLoop(
	ctx context.Context,
	path string,
	receiver chan<- *ListResponse,
) {
	defer e.ctr.BumpSum("ephemeral.etcd.listwatch.done", 1)
	// use ctx to stop inner goroutine
	innerCtx, cancel := context.WithCancel(ctx)
	e.cancelLock.Lock()
	e.listCancel[innerCtx] = cancel
	e.cancelLock.Unlock()
	// stop cancel the routine if we can, and remove cancel from list
	stop := func() {
		e.cancelLock.Lock()
		if c, ok := e.listCancel[innerCtx]; ok {
			c()
			delete(e.listCancel, innerCtx)
		}
		e.cancelLock.Unlock()
	}

	// this goroutine really watch for path, then set the event on ephemeral dir except heartbeat.
	// the target is to reduce the etcd overhead caused by lots of listwatches.
	go func() {
		e.listMonitor(innerCtx, path)
		stop()
	}()

	// this routine is to receive real event under ephemeral dir and get newest snapshot of path.
	go func() {
		e.listWatcher(innerCtx, ephemeralPath(path), receiver)
		stop()
	}()
	return
}

// listMonitor is to monitor the absolute path of given path. its target is to deal with
// lots of set event of heartbeat refresh. there is only one machine which will to watch
// the path, and apply event except heartbeat on the shawdow path /watcherDir/path. Thus
// watch events etcd needs to send can be reduced significantly.
func (e *EtcdEphemeral) listMonitor(ctx context.Context, path string) {
	// get master with acquring lock
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	lock, err := etcdlock.NewMaster(e.client, p.Join(path, watcherLock), id, listMasterLockTTL)
	if err != nil {
		e.ctr.BumpSum("ephemeral.etcd.listwatch.lock.err", 1)
		log.Errorf("[etcd] create etcd lock on <%s> failed, err:%v", path, err)
		return
	}
	lock.Start()
	defer lock.Stop()
	lockCh := lock.EventsChan()
GETLOCK:
	for {
		select {
		case lev := <-lockCh:
			if lev.Type == etcdlock.MasterAdded {
				break GETLOCK
			}
		case <-ctx.Done():
			return
		}
	}
	e.ctr.BumpSum("ephemeral.etcd.listwatch.monitor.master", 1)
	// NOTE: infoFunc is for unittest
	infoFunc("[etcd] listWatch monitor start on <%s>", path)

	// prepare for monitoring
	event := e.childrenWatch(ctx, path)
	// delHandler deletes nodes in ephDir but not path
	delHandler := func(c string, isDir bool) error {
		dPath := ephemeralPath(p.Join(path, c))
		if err := e.etcdDelete(dPath, true); err != nil {
			e := fmt.Errorf("[etcd] listwatch monitor delete <%s> failed, err:%s", dPath, err)
			log.Error(e)
			return e
		}
		return nil
	}
	// addHandler create nodes in path but not ephDir
	addHandler := func(c string, isDir bool) error {
		cPath := ephemeralPath(p.Join(path, c))
		if err := e.etcdSet(cPath, "", isDir); err != nil {
			e := fmt.Errorf("[etcd] listwatch monitor create(Dir:%v) <%s> failed, err:%s", isDir, cPath, err)
			log.Error(e)
			return e
		}
		return nil
	}

	isInit := false
	for {
		select {
		case ev := <-event:
			if ev.Err != nil {
				log.Infof("[etcd] listwatch monitor on <%s> stop, err:%v", path, ev.Err)
				return
			}
			// when master gets lock, there could be node has created and started heartbeat.
			// hence, we should do diff(init) when start monitor first time.
			if ev.Resp.Action == "set" && ev.Resp.PrevNode != nil && isInit {
				// heartbeat update, ignore
				continue
			}
			log.Infof("[etcd] listwatch monitor get event: %s, node: %v", ev.Resp.Action, ev.Resp.Node)
			var err error
			var resp *etcd.Response
			if resp, err = e.client.Get(path, true, false); err != nil {
				log.Errorf("[etcd] listwatch monitor get path<%s> fail, err:%v", path, err)
				return
			}
			_, child := toChildren(path, resp)
			if resp, err = e.client.Get(ephemeralPath(path), true, false); err != nil {
				log.Errorf("[etcd] listwatch monitor get ephPath<%s> fail, err:%v", ephemeralPath(path), err)
				return
			}
			_, edChild := toChildren(ephemeralPath(path), resp)
			if err := doDiff(edChild, child, addHandler); err != nil {
				log.Errorf("[etcd] listwatch monitor apply addHandler on <%s>, err:%v", path, err)
				return
			}
			if err := doDiff(child, edChild, delHandler); err != nil {
				log.Errorf("[etcd] listwatch monitor apply delHandler on <%s>, err:%v", path, err)
				return
			}
			isInit = true
		case lev := <-lockCh:
			// lost the lock, retry
			if lev.Type == etcdlock.MasterDeleted {
				log.Errorf("[etcd] lost lock on <%s>, retry get lock", path)
				goto GETLOCK
			}
		}
	}
}

// listWatcher is to watch the shadow path of path which is /watcherDir/path.
// with listMonitor, there are real events occuring in /watcherDir/path. listWatcher is
// to receive those events, get the newest snapshot of path, and then send to caller of List.
func (e *EtcdEphemeral) listWatcher(
	ctx context.Context,
	path string,
	receiver chan<- *ListResponse,
) {
	defer close(receiver)
	event := e.childrenWatch(ctx, path)
	ev := <-event
	if ev.Err != nil {
		receiver <- &ListResponse{Err: translateEtcdErr(ev.Err)}
		return
	}
	// send first list to caller
	child, _ := toChildren(path, ev.Resp)
	receiver <- &ListResponse{Children: child}
	for ev := range event {
		if ev.Err != nil {
			log.Infof("receive error, listwatch<%s> is going to stop", path)
			e.ctr.BumpSum("ephemeral.etcd.listwatch.watch.err", 1)
			receiver <- &ListResponse{Err: translateEtcdErr(ev.Err)}
			return
		}
		// got event, to get snapshot of path, notice that it's path not ephemeralPath(path)
		log.Infof("[etcd] receive real event on <%s> :{%s:%v}", path, ev.Resp.Action, ev.Resp.Node)
		resp, err := e.client.Get(path, true, false)
		c, _ := toChildren(path, resp)
		receiver <- &ListResponse{Children: c, Err: translateEtcdErr(err)}
		e.ctr.BumpSum("ephemeral.etcd.listwatch.watch.event", 1)
	}
}

func (e *EtcdEphemeral) etcdSet(path, value string, isDir bool) error {
	var err error
	if isDir {
		if _, err = e.client.CreateDir(path, 0); translateEtcdErr(err) == ErrPathExists {
			err = nil
		}
	} else {
		_, err = e.client.Set(path, value, 0)
	}
	return err
}

func (e *EtcdEphemeral) etcdDelete(path string, recursive bool) error {
	_, err := e.client.Delete(path, recursive)
	return err
}

// watchEvent is a wrapper for etcd watch event
type watchEvent struct {
	Resp *etcd.Response
	Err  error
}

// childrenWatch is to get children and start watch.
// to deal with etcd 401 error, it will retry get and watch.
// if there is other error except 401, the return chan will be closed.
func (e *EtcdEphemeral) childrenWatch(ctx context.Context, path string) <-chan *watchEvent {
	// given 1000 buffer to reduce etcd event outdated possibilty
	ch := make(chan *watchEvent, watchBufferSize)
	// get first to acquire watch index
	resp, err := e.client.Get(path, true, false)
	if err != nil {
		log.Errorf("[etcd] childrenWatch get failed, err:%v", err)
		ch <- &watchEvent{Err: translateEtcdErr(err)}
		close(ch)
		return ch
	}
	// prepare for watch
	watchIndex := resp.EtcdIndex + 1
	stop := make(chan bool)
	watchExit := make(chan bool)
	// check if needs to stop watch
	go func() {
		select {
		// recv done, stop watch
		case <-ctx.Done():
			log.Infof("[etcd] childrenWatch<%s> get stop watch.", path)
			close(stop)
		// recv watch exit, just leave
		case <-watchExit:
		}
	}()
	// start watch
	go func() {
		for {
			// 1. send first time, 2. send event
			ch <- &watchEvent{Resp: resp}
			// use non block watch to prevent passing channel
			resp, err = e.client.Watch(path, watchIndex, true, nil, stop)
			e.ctr.BumpSum("ephemeral.etcd.childwatch.event", 1)
			if err == nil {
				watchIndex = resp.Node.ModifiedIndex + 1
				continue
			}
			// error occurs, check if 401 and need to retry
			if ee, ok := err.(*etcd.EtcdError); ok && ee.ErrorCode == etcdErr.EcodeEventIndexCleared {
				e.ctr.BumpSum("ephemeral.etcd.childwatch.outdate", 1)
				log.Warningf("[etcd] childrenwatch outdate with idx:%v, err:%v", watchIndex, err)
				// the watchIndex should be the index of returned error index which etcd uses to
				// indicate current index.
				watchIndex = ee.Index + 1
				// sleep for a while to mitigate etcd loading and ensure we can get without failure
				time.Sleep(watchRetryDuration)
				if resp, err = e.client.Get(path, true, false); err == nil {
					// NOTE: return resp to inform caller that watch has retried,
					// the resp is trigger for caller to refresh the list snapshot of path,
					// so that children are newest if there are missing event within previous outdated watch.
					continue
				}
			}
			// other err, close ch to inform caller
			ch <- &watchEvent{Err: err}
			close(watchExit)
			close(ch)
			return
		}
	}()
	return ch
}

// FIXME: Find a better way to compare etcd error.
func translateEtcdErr(err error) error {
	if err == nil {
		return err
	}
	log.Infof("receive error: %v", err)
	if err == etcd.ErrWatchStoppedByUser {
		return ErrWatchStoppedByUser
	}
	e, ok := err.(*etcd.EtcdError)
	if !ok {
		log.Errorf("[etcd] translate error code error. Err: %v", err)
		return err
	}
	switch e.ErrorCode {
	case etcdErr.EcodeKeyNotFound:
		return ErrPathNotFound
	case etcdErr.EcodeNodeExist:
		return ErrPathExists
	case etcdErr.EcodeNotFile:
		return ErrPathNotFound
	default:
		return err
	}
}

// toChildren is convert etcd children with full path to basename like result of ls.
func toChildren(path string, resp *etcd.Response) ([]string, map[string]bool) {
	children := []string{}
	isDir := make(map[string]bool)
	if resp == nil {
		return nil, nil
	}
	for _, v := range resp.Node.Nodes {
		// NOTE: truncate path to make it more like `ls`
		c := (v.Key)[len(path)+1:]
		children = append(children, c)
		isDir[c] = v.Dir
	}
	return children, isDir
}

func ephemeralPath(path string) string {
	return p.Join(WatcherDir, path)
}

// handle children in target but not in base
func doDiff(base, target map[string]bool, handler func(string, bool) error) error {
HANDLE:
	for c, dir := range target {
		if _, ok := base[c]; !ok {
			var err error
			for i := 0; i < doHandlerRetry; i++ {
				if err = handler(c, dir); err == nil {
					// no error, handle next target
					continue HANDLE
				}
			}
			// error, return it
			return err
		}
	}
	return nil
}
