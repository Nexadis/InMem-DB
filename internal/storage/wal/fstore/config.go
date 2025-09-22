package fstore

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

var sizeMap = map[string]uint64{
	"B":  1,
	"KB": 1 << 10,
	"MB": 1 << 20,
	"GB": 1 << 30,
	"TB": 1 << 40,
}

func parseSize(size string) (uint64, error) {
	var sizeScale string
	nums := make([]rune, 0, len(size))
	for i, r := range size {
		if unicode.IsDigit(r) {
			nums = append(nums, r)
			continue
		}
		sizeScale = strings.ToUpper(size[i:])
		break
	}
	scale, ok := sizeMap[sizeScale]
	if !ok {
		return 0, fmt.Errorf("invalid size: %q", size)
	}
	cnt, err := strconv.ParseUint(string(nums), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not numeric size: %w", err)
	}
	return cnt * scale, nil
}
