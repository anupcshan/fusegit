package fusegit

import (
	"log"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// Default time we want an entry to be cached in the kernel.
// In ~ all cases so far, this number should be as high as we can possibly set. We should be
// explicilty pushing invalidation notices to the kernel for entries when they change.
const DefaultCacheTimeout = 300 * time.Hour

const CtlFile = ".fusegitctl"

type repository interface {
	BlobObject(h plumbing.Hash) (*object.Blob, error)
	TreeObject(h plumbing.Hash) (*object.Tree, error)
}

type gitTreeContext struct {
	overlayRoot string
	repo        repository
	socketPath  []byte
}

func printTimeSince(action string, start time.Time) {
	if false {
		log.Printf("Completed %s in %s", action, time.Since(start))
	}
}
