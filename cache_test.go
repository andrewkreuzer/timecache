package main

import (
	"fmt"
	"gocache/deploymentpb"
	"log"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func (dbm TestBlockManager) CreateBlock(
	values []*deploymentpb.Deployment,
) []byte {
	deployments := &deploymentpb.Deployments{Deployments: values}
	deps, err := proto.Marshal(deployments)
	if err != nil {
		log.Fatalln("Failed to encode deployment:", err)
	}
	return deps
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

func put(n int, cache *Cache[string, []byte, *deploymentpb.Deployment]) (key string, size int64) {
	tstart := time.Now()
	var tend time.Time
	var deployments deploymentpb.Deployments
	for j := 0; j < n; j++ {
		t := tstart.Add(time.Duration(j*30) * time.Second)
		deployment := genDeployment(genRowKey(t))
		deployments.Deployments = append(deployments.Deployments, &deployment)
		tend = t
	}
	size = int64(proto.Size(&deployments))
	out, _ := proto.Marshal(&deployments)
	key = genCacheKey(tstart, tend)
	cache.put(key, out, size)
	return
}

func add(n int, cache *Cache[string, []byte, *deploymentpb.Deployment]) int64 {
	tstart := time.Now()
	var tsize int64
	for j := 0; j < n; j++ {
		tkey := tstart.Add(time.Duration(-j*30) * time.Second)
		deployment := genDeployment(genRowKey(tkey))
		size := int64(proto.Size(&deployment))
		tsize += size
		cache.Add(tkey, &deployment, size)
	}

	return tsize
}

func TestAdd1(t *testing.T) {
	cache := setup()
	tstart := time.Now()
	deployment := genDeployment(genRowKey(tstart))
	size := int64(proto.Size(&deployment))
	cache.Add(tstart, &deployment, size)
	if cache.ActiveLen() != 1 {
		t.Errorf("cache.Len() = %d, want %d", cache.Len(), 1)
	}
}

func TestCauseBlockCreation(t *testing.T) {
	cache := setup()
	add(16, cache)
	if cache.Len() < 1 {
		t.Errorf("cache.Len() = %d, want %d, active Len %d", cache.Len(), 1, cache.ActiveLen())
	}
}

func TestPut(t *testing.T) {
	cache := setup()
	put(20, cache)
	if cache.Len() != 1 {
		t.Errorf("cache.Len() = %d, want %d", cache.Len(), 1)
	}
}

func TestGet(t *testing.T) {
	cache := setup()
	key, _ := put(10, cache)
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

func BenchmarkAdd(b *testing.B) {
	b.ReportAllocs()
	cache := setup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		size := add(10, cache)
		b.SetBytes(size)
	}
}

func BenchmarkAddCauseBlockCreation(b *testing.B) {
	b.ReportAllocs()
	cache := setup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		size := add(50, cache)
		b.SetBytes(size)
	}
}

func BenchmarkGet(b *testing.B) {
	cache := setup()
	key, _ := put(100, cache)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(key)
	}
}

func BenchmarkPut(b *testing.B) {
	b.ReportAllocs()
	cache := setup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, size := put(30, cache)
		b.SetBytes(size)
	}
}

func BenchmarkFetchParsingCombinedBlocks(b *testing.B) {
	b.ReportAllocs()
	cache := setup()
	put(10, cache)
	fetchRequests := cache.Fetch("")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var deployments []byte
		for _, req := range fetchRequests {
			deployments = append(deployments, req...)
		}

		b.SetBytes(int64(len(deployments)))
		var deps deploymentpb.Deployments
		proto.Unmarshal(deployments, &deps)
	}
}

func BenchmarkFetchParsingEachBlock(b *testing.B) {
	b.ReportAllocs()
	cache := setup()
	put(10, cache)
	fetchRequests := cache.Fetch("")
	var tsize int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var deployments []*deploymentpb.Deployment
		for _, req := range fetchRequests {
			var deps deploymentpb.Deployments
			tsize += int64(len(req))
			proto.Unmarshal(req, &deps)
			deployments = append(deployments, deps.Deployments...)
		}
		b.SetBytes(tsize)
		tsize = 0
	}
}

func genCacheKey(tstart, tend time.Time) string {
	keystart := ^uint64(0) - uint64(tstart.UnixNano())
	keyend := ^uint64(0) - uint64(tend.UnixNano())
	return fmt.Sprintf("%017d:%017d", keyend, keystart)
}

func genDeployment(key string) deploymentpb.Deployment {
	return deploymentpb.Deployment{
		RowKey:      key,
		Name:        "test",
		UUID:        &deploymentpb.UUID{Value: "1234567890"},
		Environment: "test",
		Duration:    durationpb.New(1 * time.Minute),
		CreatedAt:   timestamppb.New(time.Now()),
		Status:      deploymentpb.Deployment_STARTED,
	}
}

func genRowKey(t time.Time) string {
	return fmt.Sprintf("%d", ^uint64(0)-uint64(t.UnixNano()))
}
