package fusegit

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/hanwen/go-fuse/v2/fs"
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
	debug       bool
	overlayRoot string
	repo        repository
	socketPath  []byte
}

func noop() {}

func (g *gitTreeContext) logCall(ctx context.Context, ino fs.Inode, format string, a ...interface{}) func() {
	if g.debug {
		start := time.Now()
		fmtStr := fmt.Sprintf(format, a...)
		pth := ino.Path(nil)
		log.Printf("=> (/%s) %s", pth, fmtStr)
		return func() {
			log.Printf("<= (/%s) %s [%s]", pth, fmtStr, time.Since(start))
		}
	}

	return noop
}
