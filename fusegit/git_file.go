package fusegit

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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
	name      string
	treeCtx   *gitTreeContext
	blobHash  plumbing.Hash
	cached    bool
	cachedObj *object.Blob

	replicatedObj *overlayFile
}

var _ fs.NodeSetattrer = (*gitFile)(nil)

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

func (f *gitFile) ensureReplicated() error {
	// Taking a lock while performing a pretty heavy IO operation is bad. But we're taking this lock over the whole thing for simplicity.
	// Might be possible to break it down for better perf in the future, if required.
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.replicatedObj != nil {
		return nil
	}

	f.replicatedObj = &overlayFile{
		treeCtx:      f.treeCtx,
		relativePath: f.Path(nil),
	}
	_, parentDirInode := f.Parent()
	parentGitTreeInode := parentDirInode.Operations().(*gitTreeInode)
	if err := parentGitTreeInode.replicateTreeInOverlay(); err != nil {
		log.Println("Error replicating", err)
		return err
	}

	// TODO: Preserve mode from original file
	mode := uint32(0666)

	if newF, err := os.OpenFile(filepath.Join(f.treeCtx.overlayRoot, f.Path(nil)), os.O_WRONLY|os.O_CREATE, os.FileMode(mode)); err != nil {
		log.Println("Error opening file", err)
		return err.(syscall.Errno)
	} else {
		reader, err := f.cachedObj.Reader()
		if err != nil {
			log.Println("Error opening git reader", err)
			return syscall.EIO
		}
		if _, err := io.Copy(newF, reader); err != nil {
			log.Println("Error copying", err)
			return syscall.EIO
		}

		_ = newF.Close()
	}

	ch := parentGitTreeInode.NewPersistentInode(context.Background(), f.replicatedObj, fs.StableAttr{Mode: mode})
	log.Println(parentGitTreeInode.RmChild(f.name))
	parentGitTreeInode.NotifyEntry(f.name)
	parentGitTreeInode.inodeCache.Delete(f.name)
	parentGitTreeInode.inodeCache.Upsert(f.name, mode, ch)

	return nil
}

func (f *gitFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if fh != nil {
		return fh.(fs.FileSetattrer).Setattr(ctx, in, out)
	}

	if f.cacheAttrs() != nil {
		return syscall.EAGAIN
	}

	if f.ensureReplicated() != nil {
		return syscall.EAGAIN
	}

	return f.replicatedObj.Setattr(ctx, fh, in, out)
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

func (f *gitFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	if f.ensureReplicated() != nil {
		return 0, syscall.EAGAIN
	}

	return f.replicatedObj.Write(ctx, f, data, off)
}
