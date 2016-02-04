package ephemeral

import (
	"fmt"
	"testing"
	"time"

	"github.com/csigo/test"
	"github.com/stretchr/testify/suite"
)

func TestZKSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping zk shard test in short mode.")
	}
	launcher := test.NewServiceLauncher()
	z := &zkSuite{}
	z.testZK = true
	z.startFunc = func() (int, error) {
		p, _, e := launcher.Start(test.ZooKeeper)
		return p, e
	}
	z.stopFunc = func() {
		launcher.StopAll()
	}
	z.newEphemeral = func(port int) (Ephemeral, error) {
		return NewZKEphemeral([]string{fmt.Sprintf(":%d", port)}, ZKRecvTimeout(time.Second))
	}
	fmt.Println("Starting ZK test suite")
	suite.Run(t, z)
}

type zkSuite struct {
	ephemeralSuite
}
