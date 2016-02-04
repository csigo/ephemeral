package ephemeral

import (
	"fmt"
	"path"
	"time"

	pf "github.com/csigo/portforward"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"
)

type ephemeralSuite struct {
	suite.Suite
	newEphemeral func(int) (Ephemeral, error)
	startFunc    func() (int, error)
	stopFunc     func()
	ephemeral    Ephemeral
	testZK       bool
	testETCD     bool
	port         int
}

func (s *ephemeralSuite) SetupSuite() {
	var err error
	s.port, err = s.startFunc()
	assert.NoError(s.T(), err)
}

func (s *ephemeralSuite) TearDownSuite() {
	s.stopFunc()
}

func (s *ephemeralSuite) SetupTest() {
	var err error
	s.ephemeral, err = s.newEphemeral(s.port)
	assert.NoError(s.T(), err)
}

func (s *ephemeralSuite) TearDownTest() {
	assert.NoError(s.T(), s.ephemeral.Close())
}

func (s *ephemeralSuite) TestAddList() {
	testDir := "/add-list"
	testNode := testDir + "/add-node"

	// testDir should be empty at the beginning
	receiver := s.ephemeral.List(context.Background(), testDir, false)
	resp := <-receiver
	assert.Error(s.T(), resp.Err)
	assert.Equal(s.T(), []string(nil), resp.Children)
	_, ok := <-receiver
	assert.False(s.T(), ok)

	// Now add testDir
	err := s.ephemeral.AddDir(context.Background(), testDir)
	assert.NoError(s.T(), err)

	// Get again. Now should be available.
	receiver = s.ephemeral.List(context.Background(), testDir, false)
	resp = <-receiver
	assert.NoError(s.T(), resp.Err)
	assert.Equal(s.T(), []string{}, resp.Children)
	// resp should be closed.
	_, ok = <-receiver
	assert.False(s.T(), ok)

	// Now add testNode
	err = s.ephemeral.AddKey(context.Background(), testNode, "")
	assert.NoError(s.T(), err)

	// Get again. Now should get testNode.
	receiver = s.ephemeral.List(context.Background(), testDir, false)
	resp = <-receiver
	assert.NoError(s.T(), resp.Err)
	assert.Equal(s.T(), []string{"add-node"}, resp.Children)
	// resp should be closed.
	_, ok = <-receiver
	assert.False(s.T(), ok)
}

func (s *ephemeralSuite) TestListWatch() {
	testDir := "/list-watch"

	// Add testDir
	err := s.ephemeral.AddDir(context.Background(), testDir)
	assert.NoError(s.T(), err)

	// Get dir and watch
	ctx, cancel := context.WithCancel(context.Background())
	receiver := s.ephemeral.List(ctx, testDir, true)
	n := <-receiver
	assert.NoError(s.T(), n.Err)
	assert.Equal(s.T(), []string{}, n.Children)

	// nothing else should pop up from receiver side
	select {
	case <-receiver:
		assert.Fail(s.T(), "shouldn't receive")
	case <-time.After(time.Second):
	}

	// Add a children
	err = s.ephemeral.AddKey(context.Background(), testDir+"/node1", "")
	assert.NoError(s.T(), err)

	// We should receive node from receiver.
	select {
	case nodes := <-receiver:
		assert.NoError(s.T(), nodes.Err)
		assert.Equal(s.T(), []string{"node1"}, nodes.Children)
	case <-time.After(time.Second):
		assert.Fail(s.T(), "should receive")
	}

	// Cancel the watch request
	cancel()
	time.Sleep(3 * time.Second)

	// make some changes
	err = s.ephemeral.AddKey(context.Background(), testDir+"/node2", "")
	assert.NoError(s.T(), err)

	// receiver should be closed.
	select {
	case r, ok := <-receiver:
		assert.True(s.T(), ok)
		assert.Equal(s.T(), ErrWatchStoppedByUser, r.Err)
	case <-time.After(time.Second):
		assert.Fail(s.T(), "should receive")
	}
}

func (s *ephemeralSuite) TestListWatchFailed() {
	testDir := "/list-watch-failed"

	// testdir should be empty
	receiver := s.ephemeral.List(context.Background(), testDir, true)
	n := <-receiver
	assert.Error(s.T(), n.Err)
	assert.Equal(s.T(), []string(nil), n.Children)

	// nothing should pop up from receiver side
	select {
	case _, ok := <-receiver:
		assert.False(s.T(), ok)
	case <-time.After(time.Second):
		assert.Fail(s.T(), "should receive")
	}
}

func (s *ephemeralSuite) TestWatchDisconnect() {
	if s.testZK {
		fmt.Println("this is skipped in zk due to samuel library issue.")
		return
	}

	testDir := "/watch-disconnect"

	// Make a node
	assert.NoError(s.T(), s.ephemeral.AddDir(context.Background(), testDir))

	// Configure port forwarding to test network disconnection
	portForward := 4444
	stop, err := pf.PortForward(fmt.Sprintf(":%d", portForward), fmt.Sprintf(":%d", s.port))
	assert.NoError(s.T(), err, "no error")

	// Make a new storer to be disconnected later.
	eph2, err := s.newEphemeral(portForward)
	assert.NoError(s.T(), err)

	// List with watch. It should return current children
	children := eph2.List(context.Background(), testDir, true)
	c := <-children
	assert.NoError(s.T(), c.Err)
	assert.Equal(s.T(), []string{}, c.Children)

	// make sure that etcd watch has started
	time.Sleep(3 * time.Second)
	// Now disconnect
	close(stop)

	// list channel should get error and close.
	select {
	case c := <-children:
		assert.Error(s.T(), c.Err)
		assert.Equal(s.T(), []string(nil), c.Children)
	case <-time.After(5 * time.Second):
		assert.Fail(s.T(), "should receive")
	}
	select {
	case _, ok := <-children:
		assert.False(s.T(), ok)
	case <-time.After(5 * time.Second):
		assert.Fail(s.T(), "should receive close")
	}

}

func (s *ephemeralSuite) TestGet() {
	testPath := "/test"
	testValue := "studio"

	// create node with value
	err := s.ephemeral.AddKey(context.Background(), testPath, testValue)
	assert.NoError(s.T(), err)

	// test get if works
	v, err := s.ephemeral.Get(context.Background(), testPath)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), testValue, v)
}

func (s *ephemeralSuite) TestListWatchClose() {
	testDir := "/list-watch-close"
	testNode := "node1"

	// Add testDir
	err := s.ephemeral.AddDir(context.Background(), testDir)
	assert.NoError(s.T(), err)
	receiver := s.ephemeral.List(context.Background(), testDir, true)
	select {
	case n, ok := <-receiver:
		assert.True(s.T(), ok)
		assert.NoError(s.T(), n.Err)
		assert.Equal(s.T(), []string{}, n.Children)
	case <-time.After(5 * time.Second):
		assert.Fail(s.T(), "should receive")
	}

	// create node with value
	eph2, err := s.newEphemeral(s.port)
	assert.NoError(s.T(), err)
	err = eph2.AddKey(context.Background(), path.Join(testDir, testNode), "")
	assert.NoError(s.T(), err)

	n := <-receiver
	assert.NoError(s.T(), n.Err)
	assert.Equal(s.T(), []string{testNode}, n.Children)

	// Close ephemeral and wait
	assert.NoError(s.T(), eph2.Close())
	select {
	case n, ok := <-receiver:
		assert.True(s.T(), ok)
		assert.NoError(s.T(), n.Err)
		assert.Equal(s.T(), []string{}, n.Children)
	case <-time.After(5 * time.Second):
		assert.Fail(s.T(), "should receive")
	}
}

func (s *ephemeralSuite) TestGetDir() {
	testPath := "/test"

	// create a dir, sleep for a while to wait for previous test end
	time.Sleep(time.Duration(2*DefaultEphemeralTTL) * time.Second)
	err := s.ephemeral.AddDir(context.Background(), testPath)
	assert.NoError(s.T(), err)

	// get dir should return error
	_, err = s.ephemeral.Get(context.Background(), testPath)
	assert.Error(s.T(), err)
}

func (s *ephemeralSuite) TestGetNonExistPath() {
	testPath := "/NONEXIST-haha"
	// get a non-exist node
	_, err := s.ephemeral.Get(context.Background(), testPath)
	assert.Equal(s.T(), ErrPathNotFound, err)
}
