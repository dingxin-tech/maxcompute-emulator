package wire

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Count retained Go values, including NULL slots and collection overhead; a
// small encoded payload is not a bound on its decoded memory footprint.
func retainedBytes(v any) int64 {
	switch x := v.(type) {
	case string:
		return int64(len(x)) + 16
	case []byte:
		return int64(len(x)) + 24
	case []any:
		n := int64(24 + 16*len(x))
		for _, item := range x {
			n += retainedBytes(item)
		}
		return n
	case map[any]any:
		n := int64(48 + 32*len(x))
		for k, item := range x {
			n += retainedBytes(k) + retainedBytes(item)
		}
		return n
	case map[string]any:
		n := int64(48 + 32*len(x))
		for k, item := range x {
			n += retainedBytes(k) + retainedBytes(item)
		}
		return n
	default:
		return 32
	}
}

// Arrow IPC buffers may be compressed internally, independently of HTTP
// Content-Encoding. Reject their allocations before decompression can expand
// beyond the request budget. DecodeArrow converts this panic into InvalidData.
type budgetAllocator struct {
	mu   sync.Mutex
	used int64
}

func (a *budgetAllocator) reserve(delta int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if delta > MaxPayload-a.used {
		panic(fmt.Errorf("decoded Arrow buffers exceed 64 MiB"))
	}
	a.used += delta
}

func (a *budgetAllocator) Allocate(size int) []byte {
	a.reserve(int64(size))
	return memory.DefaultAllocator.Allocate(size)
}

func (a *budgetAllocator) Reallocate(size int, b []byte) []byte {
	a.reserve(int64(size - len(b)))
	return memory.DefaultAllocator.Reallocate(size, b)
}

func (a *budgetAllocator) Free(b []byte) {
	a.reserve(-int64(len(b)))
	memory.DefaultAllocator.Free(b)
}
