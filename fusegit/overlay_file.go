package fusegit

import (
	"context"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type overlayFile struct {
	fs.Inode

	treeCtx      *gitTreeContext
	relativePath string
}

func (g *overlayFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if fh != nil {
		return fh.(fs.FileSetattrer).Setattr(ctx, in, out)
	}

	// NOTE: This is a cop-out way of implementing Setattr instead of copying the code verbatim from LoopbackFile.
	fd, err := syscall.Open(filepath.Join(g.treeCtx.overlayRoot, g.relativePath), syscall.O_RDONLY, 0)
	if err != nil {
		return err.(syscall.Errno)
	}

	defer syscall.Close(fd)
	return fs.NewLoopbackFile(fd).(fs.FileSetattrer).Setattr(ctx, in, out)
}
