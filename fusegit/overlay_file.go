package fusegit

import (
	"context"
	"log"
	"os"
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

	if mode, ok := in.GetMode(); ok {
		if err := os.Chmod(f.fullPath(), os.FileMode(mode)); err != nil {
			return err.(syscall.Errno)
		}
	}

	if size, ok := in.GetSize(); ok {
		if err := syscall.Truncate(f.fullPath(), int64(size)); err != nil {
			return err.(syscall.Errno)
		}
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(f.fullPath(), &st); err != nil {
		return err.(syscall.Errno)
	}
	out.FromStat(&st)
	out.SetTimeout(DefaultCacheTimeout)

	return 0
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

func (f *overlayFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	if fh != nil {
		if _, ok := fh.(fs.FileWriter); ok {
			return fh.(fs.FileWriter).Write(ctx, data, off)
		}
	}

	fd, err := syscall.Open(f.fullPath(), syscall.O_RDWR, 0)
	if err != nil {
		log.Println("Error opening", err)
		return 0, err.(syscall.Errno)
	}

	defer syscall.Close(fd)
	return fs.NewLoopbackFile(fd).(fs.FileWriter).Write(ctx, data, off)
}
