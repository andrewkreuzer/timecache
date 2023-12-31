package timecache

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/andrewkreuzer/timecache/deploymentpb"
	"google.golang.org/protobuf/proto"
)

type BlockManager[K comparable, V any, A any] interface {
	CreateKey(time.Time, time.Time) K
	CreateBlock([]A) V
	ParseKey(K) (time.Time, time.Time)
	CalculateBlockSize([]A) int64
}

type DefaultBlockManager struct{}

func (m *DefaultBlockManager) CreateKey(tstart, tend time.Time) string {
	keystart := ^uint64(0) - uint64(tstart.UnixNano())
	keyend := ^uint64(0) - uint64(tend.UnixNano())
	return fmt.Sprintf("%017d:%017d", keyend, keystart)
}

func (m *DefaultBlockManager) ParseKey(key string) (time.Time, time.Time) {
	k, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		log.Println(err)
	}
	t := time.Unix(0, ^int64(k))
	return t, t
}

func (m *DefaultBlockManager) CreateBlock(
	values []*deploymentpb.Deployment,
) []byte {
	deployments := &deploymentpb.Deployments{Deployments: values}
	deps, err := proto.Marshal(deployments)
	if err != nil {
		log.Fatalln("Failed to encode deployment:", err)
	}
	return deps
}

func (m *DefaultBlockManager) CalculateBlockSize(
	values []*deploymentpb.Deployment,
) int64 {
	deployments := &deploymentpb.Deployments{Deployments: values}
	return int64(proto.Size(deployments))
}
