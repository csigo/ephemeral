package ephemeral

import (
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/csigo/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"
)

func TestEtcdSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping zk shard test in short mode.")
	}
	launcher := test.NewServiceLauncher()
	defer launcher.StopAll()

	e := &etcdSuite{}
	e.testETCD = true
	e.startFunc = func() (int, error) {
		port, _, err := launcher.Start(test.Etcd)
		return port, err
	}
	e.stopFunc = func() {}
	e.newEphemeral = func(port int) (Ephemeral, error) {
		return NewEtcdEphemeral(etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", port)}))
	}
	fmt.Println("Starting ETCD test suite")
	suite.Run(t, e)
}

type etcdSuite struct {
	ephemeralSuite
}

func (s *etcdSuite) TestHeartbeat() {
	err := s.ephemeral.AddKey(context.Background(), "/cccc/dddd", "")
	s.NoError(err)

	// waiting first time create or set node expired
	time.Sleep(2 * time.Duration(DefaultEphemeralTTL) * time.Second)

	respCh := s.ephemeral.List(context.Background(), "/cccc", false)
	resp := <-respCh
	s.Equal([]string{"dddd"}, resp.Children)

}

func (s *etcdSuite) TestListCleanup() {
	// create alone ephemeral
	em, err := NewEtcdEphemeral(etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", s.port)}))
	assert.NoError(s.T(), err)
	ctx, cancel := context.WithCancel(context.Background())

	assert.NoError(s.T(), em.AddDir(context.Background(), "/aaa"))

	// create a routine to close ephemeral
	stop := make(chan struct{})
	go func() {
		<-stop
		cancel()
	}()

	respCh := em.List(ctx, "/aaa", true)
	// to check cancel func
	_ = em.List(context.Background(), "/aaa", true)
	// first time we should get list result
	select {
	case resp := <-respCh:
		assert.NoError(s.T(), resp.Err)
	case <-time.After(10 * time.Second):
		assert.Fail(s.T(), "should get list result first time")
	}
	// call close
	stop <- struct{}{}

	// then get stopped by user
	select {
	case resp := <-respCh:
		assert.Error(s.T(), resp.Err, "should get stopped by user")
	case <-time.After(10 * time.Second):
		assert.Fail(s.T(), "should get close done signal")
	}
	// then we should get close
	select {
	case _, ok := <-respCh:
		assert.False(s.T(), ok)
	case <-time.After(10 * time.Second):
		assert.Fail(s.T(), "should get close done signal")
	}

	s.Equal(1, len(em.listCancel), "should be still 1 cancel")
	s.NoError(em.Close())
}

func (s *etcdSuite) TestDeleteNodeWhenClosed() {
	em, _ := NewEtcdEphemeral(etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", s.port)}), EtcdEphemeralTTL(100))

	ec := etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", s.port)})
	s.NotNil(ec)

	err := em.AddKey(context.Background(), "/cccc/dddd", "")
	s.NoError(err)

	// if em is closed, the children created by it should be gone
	em.Close()
	time.Sleep(3 * time.Second)

	// the node should be deleted. don't warry about expiration because TTL is 100
	_, err = ec.Get("/cccc/dddd", true, false)
	s.Error(err)
}

// This test is to test adding key with an existing but expired node.
// For example, when we restart docker, we will meet the condition.
func (s *etcdSuite) TestAddKeyWithExpiredNode() {
	node := "/test"
	em, _ := s.newEphemeral(s.port)
	ec := etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", s.port)})
	s.NotNil(ec)

	// create a node with TTL
	ec.Set(node, "", DefaultEphemeralTTL)
	err := em.AddKey(context.Background(), node, "")
	s.NoError(err)

	// create a permanent node
	node = "/permanent"
	ec.Set(node, "", 0)
	err = em.AddKey(context.Background(), node, "")
	s.Error(err)
	_, err = ec.Delete(node, true)
	s.NoError(err)

	ec.Close()
	em.Close()
}

func (s *etcdSuite) TestWatchOutOfDate() {
	// set buffer size to 0
	oriBufferSize := watchBufferSize
	watchBufferSize = 0
	// set watched dir
	dir := "/watch-outdate"
	err := s.ephemeral.AddDir(context.Background(), dir)
	s.NoError(err)
	// create etcd client
	ec := etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", s.port)})
	s.NotNil(ec)
	// get childrenwatch
	etEM := s.ephemeral.(*EtcdEphemeral)
	event := etEM.childrenWatch(context.Background(), dir)
	select {
	case ev := <-event:
		s.NoError(ev.Err)
		s.Equal(0, len(ev.Resp.Node.Nodes))
	case <-time.After(10 * time.Second):
		s.Fail("should get set event")
	}
	// set key for 1100 time to make etcd watch buffer gone
	for i := 0; i < 1100; i++ {
		_, err := ec.Set(path.Join(dir, "node1"), strconv.Itoa(i), 0)
		s.NoError(err)
	}
	// theres should be a set event in chan, and we don't take it until set 1500 to make watch out of date
	select {
	case ev := <-event:
		s.NoError(ev.Err)
		s.Equal("set", ev.Resp.Action)
	case <-time.After(10 * time.Second):
		s.Fail("should get set event")
	}
	// then we should receive "get" and newest snapshot which indicates retry watch
	select {
	case ev := <-event:
		s.NoError(ev.Err)
		s.Equal("get", ev.Resp.Action)
		s.Equal(1, len(ev.Resp.Node.Nodes))
	case <-time.After(10 * time.Second):
		s.Fail("should get get event")
	}
	// if retry watch failed we will get another event, so we shouldn't get any event here
	select {
	case <-event:
		s.Fail("should not get any event")
	case <-time.After(10 * time.Second):
	}

	watchBufferSize = oriBufferSize
	ec.Close()
}

func TestToChildren(t *testing.T) {
	resp := &etcd.Response{
		Node: &etcd.Node{
			Key: "/root",
			Dir: true,
			Nodes: []*etcd.Node{
				&etcd.Node{Key: "/root/node1", Dir: false},
				&etcd.Node{Key: "/root/node2", Dir: false},
				&etcd.Node{Key: "/root/dir", Dir: true},
			},
		},
	}
	children, isDir := toChildren("/root", resp)
	assert.Equal(t, []string{"node1", "node2", "dir"}, children)
	assert.False(t, isDir["node1"])
	assert.False(t, isDir["node2"])
	assert.True(t, isDir["dir"])

	children, isDir = toChildren("/root", nil)
	assert.Nil(t, children)
	assert.Nil(t, isDir)
}

func (s *etcdSuite) TestInitEphemeralDirConsistency() {
	// create etcd client
	ec := etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", s.port)})
	s.NotNil(ec)
	// set data in path
	ec.Set("/consist/f1", "", 0)
	ec.SetDir("/consist/d1", 0)
	ec.SetDir("/consist/d3", 0)
	// set data in ephemeralDir/path
	ec.Set(ephemeralPath("/consist/f2"), "", 0)
	ec.SetDir(ephemeralPath("/consist/d1"), 0)
	ec.SetDir(ephemeralPath("/consist/d2"), 0)

	// call List
	ch := s.ephemeral.List(context.Background(), "/consist", true)
	// collect event until init done
	child := []string{}
COLLECT:
	for {
		select {
		case resp := <-ch:
			s.NoError(resp.Err)
			child = resp.Children
		case <-time.After(3 * time.Second):
			// just wait done
			break COLLECT
		}
	}
	// finally should be the same
	s.Equal([]string{"d1", "d3", "f1"}, child)

	// check final tree consistency
	_, err := ec.Get(ephemeralPath("/consist/"), true, false)
	s.NoError(err, "should exist")
	_, err = ec.Get(ephemeralPath("/consist/f1"), true, false)
	s.NoError(err, "should exist")
	_, err = ec.Get(ephemeralPath("/consist/d1"), true, false)
	s.NoError(err, "should exist")
	_, err = ec.Get(ephemeralPath("/consist/d3"), true, false)
	s.NoError(err, "should exist")
	_, err = ec.Get(ephemeralPath("/consist/d2"), true, false)
	s.Error(err, "shouldn't exist")
	_, err = ec.Get(ephemeralPath("/consist/f2"), true, false)
	s.Error(err, "shouldn't exist")
}

func (s *etcdSuite) TestLockMaster() {
	// mock infoFunc
	master := 0
	infoFunc = func(fmt string, arg ...interface{}) {
		master++
	}
	err := s.ephemeral.AddDir(context.Background(), "/test-lock")
	s.NoError(err)
	// create anohter ephemeral client
	eph2, err := s.newEphemeral(s.port)
	assert.NoError(s.T(), err)
	// three list on test-lock
	chs := [](<-chan *ListResponse){
		s.ephemeral.List(context.Background(), "/test-lock", true),
		s.ephemeral.List(context.Background(), "/test-lock", true),
		eph2.List(context.Background(), "/test-lock", true),
	}
	// should get empty children
	for i, c := range chs {
		select {
		case r := <-c:
			s.NoError(r.Err)
			s.Equal([]string{}, r.Children)
		case <-time.After(3 * time.Second):
			s.Fail("should get from ch", i)
		}
	}
	// should get node1
	s.ephemeral.AddKey(context.Background(), "/test-lock/node1", "")
	for i, c := range chs {
		select {
		case r := <-c:
			s.NoError(r.Err)
			s.Equal([]string{"node1"}, r.Children)
		case <-time.After(3 * time.Second):
			s.Fail("should get from ch", strconv.Itoa(i))
		}
	}
	// should get node2
	eph2.AddKey(context.Background(), "/test-lock/node2", "")
	for i, c := range chs {
		select {
		case r := <-c:
			s.NoError(r.Err)
			s.Equal([]string{"node1", "node2"}, r.Children)
		case <-time.After(3 * time.Second):
			s.Fail("should get from ch", strconv.Itoa(i))
		}
	}
	s.Equal(1, master, "there should be 1 master")
	eph2.Close()
}
