package fusegit

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type gitFile struct {
	fs.Inode

	mu        sync.Mutex
	treeCtx   *gitTreeContext
	blobHash  plumbing.Hash
	cached    bool
	cachedObj *object.Blob
}

func (f *gitFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *gitFile) cacheAttrs() error {
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

	f.cached = true
	f.cachedObj = obj

	return nil
}

func (f *gitFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f.cacheAttrs() != nil {
		return syscall.EAGAIN
	}
	out.Attr.Size = uint64(f.cachedObj.Size)
	out.SetTimeout(DefaultCacheTimeout)
	return 0
}

func (f *gitFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if f.cacheAttrs() != nil {
		return nil, syscall.EAGAIN
	}
	defer printTimeSince("Reading file", time.Now())

	r, err := f.cachedObj.Reader()
	if err != nil {
		log.Println("Error getting reader", err)
		return nil, syscall.EAGAIN
	}

	if off > 0 {
		if seeker, ok := r.(io.Seeker); ok {
			_, err = seeker.Seek(off, io.SeekStart)
			if err != nil {
				log.Println("Error skipping bytes", err)
				return nil, syscall.EAGAIN
			}
		} else {
			_, err = io.CopyN(ioutil.Discard, r, off)
			if err != nil {
				log.Println("Error skipping bytes", err)
				return nil, syscall.EAGAIN
			}
		}
	}

	n, err := r.Read(dest)
	if err != nil && err != io.EOF {
		log.Println("Error reading file", err, f.blobHash)
		return nil, syscall.EAGAIN
	}

	return fuse.ReadResultData(dest[:n]), 0
}
