package main

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"
)

type CacheConfig[K comparable, V blockType, A proto.Message] struct {
	MaxMemory    string
	MaxEntries   int
	BlockSize    string
	BlockManager BlockManager[K, V, A]
}

func convertToBytes(size string) (int64, error) {
	size, unit := parseSize(size)
	switch unit {
	case "":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s, nil
	case "KiB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1024, nil
	case "KB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1000, nil
	case "MiB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1024 * 1024, nil
	case "MB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1000 * 1000, nil
	case "GB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1000 * 1000 * 1000, nil
	case "GiB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unknown unit")
	}
}

func parseSize(size string) (string, string) {
	var unit string
	var s string
	for i := len(size) - 1; i >= 0; i-- {
		if size[i] <= '9' {
			unit = size[i+1:]
			s = size[:i+1]
			break
		}
	}
	return s, unit
}
