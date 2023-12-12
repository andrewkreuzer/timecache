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
	deployment := createDeployment(createRowKey(tstart))
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
	cache := setup()
	deps, sizes := genDeps(1)
	t := time.Now()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := range deps {
			cache.Add(t, deps[j], sizes[j])
			b.SetBytes(sizes[j])
		}
	}
}

func BenchmarkAddCauseBlockCreation(b *testing.B) {
	cache := setup()
	deps, sizes := genDeps(14)
	t := time.Now()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := range deps {
			cache.Add(t, deps[j], sizes[j])
			b.SetBytes(sizes[j])
		}
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
	cache := setup()
	keys, deps, size := genBlock(14)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := range keys {
			cache.put(keys[j], deps, size)
			b.SetBytes(size)
		}
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

func put(n int, cache *Cache[string, []byte, *deploymentpb.Deployment]) (key string, size int64) {
	tstart := time.Now()
	var tend time.Time
	var deployments deploymentpb.Deployments
	for j := 0; j < n; j++ {
		t := tstart.Add(time.Duration(j*30) * time.Second)
		deployment := createDeployment(createRowKey(t))
		deployments.Deployments = append(deployments.Deployments, &deployment)
		tend = t
	}
	size = int64(proto.Size(&deployments))
	out, _ := proto.Marshal(&deployments)
	key = createCacheKey(tstart, tend)
	cache.put(key, out, size)
	return
}

func add(n int, cache *Cache[string, []byte, *deploymentpb.Deployment]) int64 {
	tstart := time.Now()
	var tsize int64
	for j := 0; j < n; j++ {
		tkey := tstart.Add(time.Duration(-j*30) * time.Second)
		deployment := createDeployment(createRowKey(tkey))
		size := int64(proto.Size(&deployment))
		tsize += size
		cache.Add(tkey, &deployment, size)
	}

	return tsize
}

func genDeps(n int) ([]*deploymentpb.Deployment, []int64) {
	var deployments []*deploymentpb.Deployment
	var sizes []int64
	for i := 0; i < n; i++ {
		deployment := createDeployment(createRowKey(time.Now()))
		deployments = append(deployments, &deployment)
		sizes = append(sizes, int64(proto.Size(&deployment)))
	}

	return deployments, sizes
}

func genBlock(n int) ([]string, []byte, int64) {
	tstart := time.Now()
	var deployments deploymentpb.Deployments
	var keys []string
	for j := 0; j < n; j++ {
		t := tstart.Add(time.Duration(j*30) * time.Second)
		deployment := createDeployment(createRowKey(t))
		deployments.Deployments = append(deployments.Deployments, &deployment)
		keys = append(keys, createCacheKey(time.Now(), time.Now()))
	}
	out, _ := proto.Marshal(&deployments)
	size := int64(proto.Size(&deployments))
	return keys, out, size
}

func createDeployment(key string) deploymentpb.Deployment {
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

func createRowKey(t time.Time) string {
	return fmt.Sprintf("%d", ^uint64(0)-uint64(t.UnixNano()))
}

func createCacheKey(tstart, tend time.Time) string {
	keystart := ^uint64(0) - uint64(tstart.UnixNano())
	keyend := ^uint64(0) - uint64(tend.UnixNano())
	return fmt.Sprintf("%017d:%017d", keyend, keystart)
}
