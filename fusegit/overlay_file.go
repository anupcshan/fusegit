package fusegit

import (
	"context"
	"log"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type overlayFile struct {
	fs.Inode

	treeCtx      *gitTreeContext
	relativePath string
}

var _ fs.NodeSetattrer = (*overlayFile)(nil)
var _ fs.NodeOpener = (*overlayFile)(nil)

func (f *overlayFile) fullPath() string {
	return filepath.Join(f.treeCtx.overlayRoot, f.relativePath)
}

func (f *overlayFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if fh != nil {
		return fh.(fs.FileSetattrer).Setattr(ctx, in, out)
	}

	// NOTE: This is a cop-out way of implementing Setattr instead of copying the code verbatim from LoopbackFile.
	fd, err := syscall.Open(f.fullPath(), syscall.O_RDWR, 0)
	if err != nil {
		log.Println("Error opening", err)
		return err.(syscall.Errno)
	}

	defer syscall.Close(fd)
	return fs.NewLoopbackFile(fd).(fs.FileSetattrer).Setattr(ctx, in, out)
}

func (f *overlayFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if fh != nil {
		return fh.(fs.FileGetattrer).Getattr(ctx, out)
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(f.fullPath(), &st); err != nil {
		return err.(syscall.Errno)
	}
	out.FromStat(&st)
	out.SetTimeout(DefaultCacheTimeout)
	return 0
}

func (f *overlayFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	defer printTimeSince("Open", time.Now())

	fd, err := syscall.Open(f.fullPath(), int(flags), 0)
	if err != nil {
		log.Println("Error opening", err)
		return nil, 0, err.(syscall.Errno)
	}

	return fs.NewLoopbackFile(fd), 0, 0
}
