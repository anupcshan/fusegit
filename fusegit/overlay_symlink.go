package fusegit

import (
	"context"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

type overlaySymlink struct {
	fs.Inode

	treeCtx      *gitTreeContext
	relativePath string
}

var _ = (fs.NodeReadlinker)((*overlaySymlink)(nil))

func (f *overlaySymlink) fullPath() string {
	return filepath.Join(f.treeCtx.overlayRoot, f.relativePath)
}

func (f *overlaySymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	p := f.fullPath()

	for l := 256; ; l *= 2 {
		buf := make([]byte, l)
		sz, err := syscall.Readlink(p, buf)
		if err != nil {
			return nil, err.(syscall.Errno)
		}

		if sz < len(buf) {
			return buf[:sz], 0
		}
	}
}
