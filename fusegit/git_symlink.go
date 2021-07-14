package fusegit

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"syscall"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type gitSymlink struct {
	fs.Inode

	mu           sync.Mutex
	treeCtx      *gitTreeContext
	blobHash     plumbing.Hash
	cached       bool
	cachedTarget []byte
	cachedSize   int
}

var _ = (fs.NodeReadlinker)((*gitSymlink)(nil))

func (f *gitSymlink) cacheAttrs() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.cached {
		return nil
	}

	obj, err := f.treeCtx.repo.BlobObject(f.blobHash)
	if err != nil {
		log.Println("Error locating blob object")
		return err
	}

	r, err := obj.Reader()
	if err != nil {
		log.Println("Error getting reader", err)
		return err
	}

	f.cachedTarget, err = ioutil.ReadAll(r)
	if err != nil && err != io.EOF {
		log.Println("Error reading symlink", err)
		return err
	}

	f.cached = true

	return nil
}

func (f *gitSymlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f.cacheAttrs() != nil {
		return syscall.EAGAIN
	}
	out.Attr.Size = uint64(len(f.cachedTarget))
	out.SetTimeout(DefaultCacheTimeout)
	return 0
}

func (f *gitSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if f.cacheAttrs() != nil {
		return nil, syscall.EAGAIN
	}

	return f.cachedTarget, 0
}
