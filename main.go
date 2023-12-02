package main

import (
	"fmt"
	"gocache/deploymentpb"
	"log"

	// "os"
	// "runtime"
	// "runtime/pprof"

	"time"

	"google.golang.org/protobuf/proto"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	// cf, err := os.Create("cache-cpu.prof")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer cf.Close() // error handling omitted for example
	// if err := pprof.StartCPUProfile(cf); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }
	// defer pprof.StopCPUProfile()

	log.Println("memory: ", getSystemMemory()/(1024*1024*1024), "GB")
	timer := getTimer()
	t := time.Now()

	cacheConfig := &CacheConfig[string, []byte, *deploymentpb.Deployment]{
		MaxMemory:    "1MB",
		MaxEntries:   1000,
		BlockSize:    "100KB",
		BlockManager: &DefaultBlockManager{},
	}
	cache := cacheConfig.NewCache()

	log.Println("size (empty): ", cache.Size())
	log.Println("len (empty): ", cache.Len())

	tstart := t.Add(23 * time.Hour)
	for i := 0; i < 1_000_000; i++ {
		key := tstart.Add(time.Duration(i) * time.Minute)
		deployment := genDeployment(genRowKey(key))
		// calculate the size if it where to be marshalled
		size := calculateSize(&deployment)
		cache.Add(key, &deployment, size)
	}

	timer.total = time.Since(t)

	time.Sleep(1 * time.Second)
	log.Println("size: ", cache.Size()/1000, "KB")
	log.Println("len: ", cache.Len())
	latest, ok := cache.Newest()
	if !ok {
		log.Println("Failed to get latest deployments from cache")
	} else {
		var deps deploymentpb.Deployments
		proto.Unmarshal(latest.value, &deps)
		log.Printf("latest deployments %s: %d", latest.key, len(deps.Deployments))
	}
	log.Printf("active block len: %d", len(cache.Active().values))

	for i := 0; i < 10000; i++ {
		cache.Newest()
	}

	for i := 0; i < 100; i++ {
		fetchParsingEachBlock(cache, tstart)
	}

	for i := 0; i < 100; i++ {
		fetchParsingCombinedBlocks(cache, tstart)
	}

	timer.getStats()
	cache.Stats()

	// runtime.GC()
	// mf, err := os.Create("cache-mem-postgc.prof")
	// if err != nil {
	// 	log.Fatal("could not create memory profile: ", err)
	// }
	// defer mf.Close() // error handling omitted for example
	// if err := pprof.WriteHeapProfile(mf); err != nil {
	// 	log.Fatal("could not write memory profile: ", err)
	// }

}

// combining all the blocks into one and then parsing it
// seems to be significantly slower than parsing each block
// protobuf is better suited to parsing small messages
func fetchParsingCombinedBlocks(cache CacheInterface[string, []byte, *deploymentpb.Deployment], tstart time.Time) *deploymentpb.Deployments {
	defer getTimer().timer("fetchParsingCombinedBlocks")()
	key := genCacheKey(tstart, tstart.Add(-48*time.Hour))
	fetchRequests := cache.Fetch(key)
	var deployments []byte
	for _, req := range fetchRequests {
		deployments = append(deployments, req...)
	}

	var deps deploymentpb.Deployments
	proto.Unmarshal(deployments, &deps)

	return &deps
}

func fetchParsingEachBlock(cache CacheInterface[string, []byte, *deploymentpb.Deployment], tstart time.Time) []*deploymentpb.Deployment {
	defer getTimer().timer("fetchParsingEachBlock")()
	key := genCacheKey(tstart, tstart.Add(-48*time.Hour))
	fetchRequests := cache.Fetch(key)
	var deployments []*deploymentpb.Deployment
	for _, req := range fetchRequests {
		var deps deploymentpb.Deployments
		proto.Unmarshal(req, &deps)
		deployments = append(deployments, deps.Deployments...)
	}

	return deployments
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

func calculateSize(deployment proto.Message) int64 {
	return int64(proto.Size(deployment))
}
