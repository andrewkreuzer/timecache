package main

import (
	"fmt"
	"gocache/deploymentpb"
	"log"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

type TestBlockManager struct{}

func (dbm TestBlockManager) CreateKey(tstart, tend time.Time) string {
	keystart := ^uint64(0) - uint64(tstart.UnixNano())
	keyend := ^uint64(0) - uint64(tend.UnixNano())
	return fmt.Sprintf("%017d:%017d", keyend, keystart)
}

func (dbm TestBlockManager) ParseKey(key string) (time.Time, time.Time) {
	k, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		log.Println(err)
	}
	t := time.Unix(0, ^int64(k))
	return t, t
}

func (dbm TestBlockManager) CreateValue(
	values []*deploymentpb.Deployment,
) ([]byte, int64) {
	defer getTimer().timer("valueCreateFuncFull")()
	deployments := &deploymentpb.Deployments{Deployments: values}
	deps, err := proto.Marshal(deployments)
	if err != nil {
		log.Fatalln("Failed to encode deployment:", err)
	}
	return deps, calculateSize(deployments)
}

func (dbm TestBlockManager) CalculateBlockSize(
	values []*deploymentpb.Deployment,
) int64 {
	deployments := &deploymentpb.Deployments{Deployments: values}
	return int64(proto.Size(deployments))
}

func setup() *Cache[string, []byte, *deploymentpb.Deployment] {
	cacheConfig := &CacheConfig[string, []byte, *deploymentpb.Deployment]{
		MaxMemory:    "5MB",
		MaxEntries:   1000,
		BlockSize:    "1KB",
		BlockManager: TestBlockManager{},
	}
	return NewTypedCache[string, []byte, *deploymentpb.Deployment](cacheConfig)
}

func TestAdd1(t *testing.T) {
	cache := setup()
	tstart := time.Now()
	deployment := genDeployment(genRowKey(tstart))
	size := calculateSize(&deployment)
	cache.Add(tstart, &deployment, size)
	if cache.ActiveLen() != 1 {
		t.Errorf("cache.Len() = %d, want %d", cache.Len(), 1)
	}
}

func TestCauseBlockCreation(t *testing.T) {
	cache := setup()
	tstart := time.Now()
	for j := 0; j < 16; j++ {
		tkey := tstart.Add(time.Duration(-j*30) * time.Second)
		deployment := genDeployment(genRowKey(tkey))
		size := calculateSize(&deployment)
		cache.Add(tkey, &deployment, size)
	}
	// wait for the put goroutine to finish
	time.Sleep(10 * time.Microsecond)
	if cache.Len() < 1 {
		t.Errorf("cache.Len() = %d, want %d, active Len %d", cache.Len(), 1, cache.ActiveLen())
	}
}

func TestPut(t *testing.T) {
	cache := setup()
	var tend time.Time
	tstart := time.Now()
	var deployments deploymentpb.Deployments
	for j := 0; j < 10; j++ {
		key := tstart.Add(time.Duration(j*30) * time.Second)
		deployment := genDeployment(genRowKey(key))
		deployments.Deployments = append(deployments.Deployments, &deployment)
		tend = key
	}
	size := calculateSize(&deployments)
	out, err := proto.Marshal(&deployments)
	if err != nil {
		t.Errorf("Failed to encode deployment: %s", err)
	}
	key := genCacheKey(tstart, tend)
	cache.put(key, out, size)
	if cache.Len() != 1 {
		t.Errorf("cache.Len() = %d, want %d", cache.Len(), 1)
	}
}

func TestGet(t *testing.T) {
	cache := setup()
	var tend time.Time
	tstart := time.Now()
	var deployments deploymentpb.Deployments
	for j := 0; j < 10; j++ {
		key := tstart.Add(time.Duration(j*30) * time.Second)
		deployment := genDeployment(genRowKey(key))
		deployments.Deployments = append(deployments.Deployments, &deployment)
		tend = key
	}
	size := calculateSize(&deployments)
	out, err := proto.Marshal(&deployments)
	if err != nil {
		t.Errorf("Failed to encode deployment: %s", err)
	}
	key := genCacheKey(tstart, tend)
	cache.put(key, out, size)
	v, ok := cache.Get(key)
	if !ok {
		t.Errorf("cache.Get(%s) = %t, want %t", key, ok, true)
	}
	var got deploymentpb.Deployments
	proto.Unmarshal(*v, &got)
	if len(got.Deployments) != 10 {
		t.Errorf("cache.Get(%s) = %d, want %d", key, len(got.Deployments), 10)
	}
}

func TestCovertToBytes(t *testing.T) {
	tests := []struct {
		a    string
		want int64
	}{
		{
			a:    "100000",
			want: 100000,
		},
		{
			a:    "100009",
			want: 100009,
		},
		{
			a:    "1KiB",
			want: 1024,
		},
		{
			a:    "1KB",
			want: 1000,
		},
		{
			a:    "1MiB",
			want: 1048576,
		},
		{
			a:    "1MB",
			want: 1000000,
		},
		{
			a:    "1GiB",
			want: 1073741824,
		},
		{
			a:    "1GB",
			want: 1000000000,
		},
	}

	for _, test := range tests {
		got, err := convertToBytes(test.a)
		if err != nil {
			t.Errorf("convertToBytes(%s) = %d, want %d, error: %s", test.a, got, test.want, err)
		}
	}
}
