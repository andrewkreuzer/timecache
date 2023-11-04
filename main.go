package main

import (
	"context"
	"fmt"
	"gocache/deploymentpb"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	gc "github.com/golang/groupcache"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	group *gc.Group

	peerAddrs = []string{"gcache1:8080", "gcache2:8080", "gcache3:8080", "gcache4:8080", "gcache5:8080", "gcache6:8080"}
)

const (
	deploymentGroup = "deployment-group"
	cacheSize       = 1 << 20
)

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if hostname == "gcache1" {
		time.Sleep(1 * time.Second)
	}

	name := hostname + ":8080"
	pool := gc.NewHTTPPool("http://" + name)
	pool.Set(addrToURL(peerAddrs)...)

	t := time.Now()
	tPast := t.Add(-24 * time.Hour)
	testSpace := [2]int64{t.UnixNano(), tPast.UnixNano()}
	testKeys := genTestKeys(testSpace)
	deployments := genDeployments(testKeys)
	cachedKeys := make(map[string][2]int64)

	go http.ListenAndServe(name, pool)

	getter := gc.GetterFunc(func(_ context.Context, key string, dest gc.Sink) error {
		sleepOnSize(key)

		left, err := strconv.ParseInt(strings.Split(key, ":")[0], 10, 64)
		right, err := strconv.ParseInt(strings.Split(key, ":")[1], 10, 64)
		if err != nil {
			return err
		}

		cachedKeys[key] = [2]int64{left, right}
		return dest.SetProto(
			&deploymentpb.Deployments{
				Deployments: deployments,
			},
		)
	})

	group = gc.NewGroup(deploymentGroup, cacheSize, getter)

	for range deployments {
		l := rand.Int63n(t.UnixNano()-tPast.UnixNano()) + tPast.UnixNano()
		r := rand.Int63n(l-tPast.UnixNano()) + tPast.UnixNano()

		key := fmt.Sprintf("%d:%d", l, r)

		var t time.Time
		var d deploymentpb.Deployments
		switch hostname {
		case "gcache1":
			time.Sleep(500 * time.Millisecond)
			t = time.Now()
			group.Get(context.Background(), key, gc.ProtoSink(&d))
		case "gcache2":
			time.Sleep(750 * time.Millisecond)
			t = time.Now()
			group.Get(context.Background(), key, gc.ProtoSink(&d))
		case "gcache3":
			time.Sleep(1 * time.Second)
			t = time.Now()
			group.Get(context.Background(), key, gc.ProtoSink(&d))
		default:
			t = time.Now()
			group.Get(context.Background(), key, gc.ProtoSink(&d))
		}

		mainCache := group.CacheStats(gc.MainCache)
		hotCache := group.CacheStats(gc.HotCache)

		t2 := time.Now()
		report := struct {
			K   string
			T   time.Duration
			Mcg int64
			Mch int64
			Hcg int64
			Hch int64
		}{
			time.Duration(l - r).Truncate(time.Minute).String(),
			t2.Sub(t),
			mainCache.Gets,
			mainCache.Hits,
			hotCache.Gets,
			hotCache.Hits,
		}

		fmt.Printf("%+v\n", report)
	}
}

func checkCache(key string, cachedKeys map[string][2]int64) ([]string, bool) {
	left, right := parseKey(key)
	keys := make([]string, 0)
	for k, r := range cachedKeys {
		if r[0] >= left && r[1] <= right {
			keys = append(keys, k)
		}
	}

	return keys, len(keys) > 0
}

func genDeployments(keys []string) []*deploymentpb.Deployment {
	deployments := make([]*deploymentpb.Deployment, len(keys))
	for _, key := range keys {
		left, err := strconv.ParseInt(strings.Split(key, ":")[0], 10, 64)
		right, err := strconv.ParseInt(strings.Split(key, ":")[1], 10, 64)
		if err != nil {
			fmt.Println(err)
		}

		k := rand.Int63n(left-right) + right
		deployments = append(deployments, &deploymentpb.Deployment{
			Name:        fmt.Sprintf("%d", k),
			UUID:        &deploymentpb.UUID{Value: "1234567890"},
			Environment: "test",
			Duration:    durationpb.New(1 * time.Minute),
			CreatedAt:   timestamppb.New(time.Now()),
			Status:      deploymentpb.Deployment_STARTED,
		})
	}

	return deployments
}

func parseKey(key string) (int64, int64) {
	left, err := strconv.ParseInt(strings.Split(key, ":")[0], 10, 64)
	right, err := strconv.ParseInt(strings.Split(key, ":")[1], 10, 64)
	if err != nil {
		fmt.Println(err)
	}
	return left, right
}

func genTestKeys(keyEnds [2]int64) []string {
	keys := make([]string, 0)
	for len(keys) < 1000 {
		left := rand.Int63n(keyEnds[0]) + keyEnds[1]
		right := rand.Int63n(left-keyEnds[1]) + keyEnds[1]

		if right < left && left-right > 0 {
			keys = append(keys, fmt.Sprintf("%d:%d", left, right))
		}

	}

	return keys
}

func sleepOnSize(key string) {
	left, right := parseKey(key)
	d := time.Duration(left-right) / 100000
	time.Sleep(d)
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}
