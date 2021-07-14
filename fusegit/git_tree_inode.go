package fusegit

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type gitTreeInode struct {
	fs.Inode

	mu       sync.Mutex
	treeHash plumbing.Hash
	treeCtx  *gitTreeContext

	initialized bool
	isRoot      bool

	isInitializedInOverlay bool

	cached     bool
	inodeCache *inodeIndex
	inodes     []*fs.Inode
}

func NewGitTreeInode(repo repository, socketPath string, overlayRoot string) *gitTreeInode {
	return &gitTreeInode{
		isRoot: true,
		treeCtx: &gitTreeContext{
			overlayRoot: overlayRoot,
			repo:        repo,
			socketPath:  []byte(socketPath),
		},
	}
}

func (g *gitTreeInode) UpdateHash(treeHash plumbing.Hash) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cached {
		for _, k := range g.inodeCache.Dirents() {
			g.NotifyEntry(k.Name)
		}
	}

	g.treeHash = treeHash
	g.cached = false
	if g.initialized {
		g.RmAllChildren()
	}
	g.initialized = true
}

func (g *gitTreeInode) scanOverlay() error {
	overlayPath := filepath.Join(g.treeCtx.overlayRoot, g.Path(nil))
	dirents, err := os.ReadDir(overlayPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	g.isInitializedInOverlay = true

	for _, dirent := range dirents {
		if dirent.Type().IsRegular() {
			of := &overlayFile{treeCtx: g.treeCtx, relativePath: filepath.Join(g.Path(nil), dirent.Name())}
			mode := uint32(dirent.Type())
			ch := g.NewPersistentInode(context.Background(), of, fs.StableAttr{Mode: mode})
			g.inodeCache.Upsert(dirent.Name(), mode, ch)
		} else if dirent.Type() == os.ModeSymlink {
			of := &overlaySymlink{treeCtx: g.treeCtx, relativePath: filepath.Join(g.Path(nil), dirent.Name())}
			fInfo, err := dirent.Info()
			if err != nil {
				// TODO: Maybe don't fail here. This is recoverable.
				return err
			}
			mode := uint32(fInfo.Mode().Perm() | syscall.S_IFLNK)
			ch := g.NewPersistentInode(context.Background(), of, fs.StableAttr{Mode: mode})
			g.inodeCache.Upsert(dirent.Name(), mode, ch)
		}
	}

	return nil
}

func (g *gitTreeInode) scanTree() error {
	defer printTimeSince("Scan Tree", time.Now())

	tree, err := g.treeCtx.repo.TreeObject(g.treeHash)
	if err != nil {
		log.Printf("Error fetching tree object %s", g.treeHash)
		return io.ErrUnexpectedEOF
	}

	g.inodeCache = NewInodeCache()

	for _, ent := range tree.Entries {
		var mode uint32
		if ent.Mode == filemode.Submodule {
			mode = syscall.S_IFDIR
		} else {
			mode = uint32(ent.Mode)
		}

		var inode *fs.Inode
		if ent.Mode == filemode.Symlink {
			symlink := &gitSymlink{treeCtx: g.treeCtx, blobHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), symlink, fs.StableAttr{Mode: uint32(ent.Mode)})
		} else if ent.Mode.IsFile() {
			file := &gitFile{treeCtx: g.treeCtx, blobHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), file, fs.StableAttr{Mode: uint32(ent.Mode)})
		} else if ent.Mode == filemode.Submodule {
			dir := &gitTreeInode{treeCtx: g.treeCtx, treeHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), dir, fs.StableAttr{Mode: syscall.S_IFDIR})
		} else {
			dir := &gitTreeInode{treeCtx: g.treeCtx, treeHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), dir, fs.StableAttr{Mode: syscall.S_IFDIR})
		}
		g.inodeCache.Upsert(ent.Name, mode, inode)
	}

	return nil
}

func (g *gitTreeInode) cacheAttrs() error {
	if g.cached {
		return nil
	}

	if err := g.scanTree(); err != nil {
		return err
	}

	if err := g.scanOverlay(); err != nil {
		return err
	}

	if g.isRoot {
		g.inodeCache.Upsert(CtlFile, uint32(filemode.Regular),
			g.NewPersistentInode(
				context.Background(), &fs.MemRegularFile{
					Data: g.treeCtx.socketPath,
					Attr: fuse.Attr{
						Mode: 0444,
					},
				},
				fs.StableAttr{Mode: uint32(filemode.Regular)},
			),
		)
	}

	g.cached = true
	return nil
}

func (g *gitTreeInode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	defer printTimeSince("Readdir", time.Now())

	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.cacheAttrs(); err != nil {
		return nil, syscall.EAGAIN
	}

	return fs.NewListDirStream(g.inodeCache.Dirents()), 0
}

func (g *gitTreeInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer printTimeSince("Lookup", time.Now())

	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.cacheAttrs(); err != nil {
		return nil, syscall.EAGAIN
	}

	if ent := g.inodeCache.LookupInode(name); ent != nil {
		if node, ok := ent.Operations().(fs.NodeGetattrer); ok {
			var attrOut fuse.AttrOut
			node.Getattr(ctx, nil, &attrOut)
			out.Attr = attrOut.Attr
		}

		// Caching Attr values that have not been produced by `Getattr` explicitly can be problematic.
		// Currently, we don't fetch `Attr` values for directories. If we see directory attr mismatches,
		// move SetAttrTimeout below `node.Getattr` just above.
		out.SetAttrTimeout(DefaultCacheTimeout)
		out.SetEntryTimeout(DefaultCacheTimeout)
		return ent, 0
	}

	return nil, syscall.ENOENT
}

// Must be called with lock held.
func (g *gitTreeInode) replicateTreeInOverlay() error {
	if g.isInitializedInOverlay {
		// Already replicated. Nothing to do.
		return nil
	}

	if g.isRoot {
		// Top-level overlay directory guaranteed to exist. Nothing to do.
		return nil
	}

	parentName, parentInode := g.Parent()
	parentTreeInode := parentInode.Operations().(*gitTreeInode)

	if err := parentTreeInode.replicateTreeInOverlay(); err != nil {
		return err
	}

	log.Println("Creating parent dir", parentName, parentTreeInode)
	if err := syscall.Mkdir(filepath.Join(g.treeCtx.overlayRoot, g.Path(nil)), 0775); err != nil {
		log.Println("Error creating dir", err)
		return err
	}

	g.isInitializedInOverlay = true

	return nil
}

func (g *gitTreeInode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	g.mu.Lock()
	defer g.mu.Unlock()

	fullRelativePath := filepath.Join(g.Path(nil), name)
	fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)

	if err := g.replicateTreeInOverlay(); err != nil {
		return nil, nil, 0, err.(syscall.Errno)
	}

	fd, err := syscall.Creat(fullOverlayPath, mode)

	if err != nil {
		log.Println("Creating overlay file failed", err)
		return nil, nil, 0, err.(syscall.Errno)
	}

	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		syscall.Unlink(fullOverlayPath)
		return nil, nil, 0, err.(syscall.Errno)
	}
	out.FromStat(&st)

	of := &overlayFile{treeCtx: g.treeCtx, relativePath: fullRelativePath}
	ch := g.NewPersistentInode(ctx, of, fs.StableAttr{Mode: st.Mode})
	g.inodeCache.Upsert(name, mode, ch)

	fh = fs.NewLoopbackFile(fd)

	return ch, fh, 0, 0
}

func (g *gitTreeInode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fullRelativePath := filepath.Join(g.Path(nil), name)
	fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)

	log.Printf("About to create symlink from %s to %s", target, fullOverlayPath)

	if err := syscall.Symlink(target, fullOverlayPath); err != nil {
		return nil, err.(syscall.Errno)
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(fullOverlayPath, &st); err != nil {
		syscall.Unlink(fullOverlayPath)
		return nil, err.(syscall.Errno)
	}
	out.FromStat(&st)

	log.Printf("%x", st.Mode)

	of := &overlaySymlink{treeCtx: g.treeCtx, relativePath: fullRelativePath}
	ch := g.NewPersistentInode(ctx, of, fs.StableAttr{Mode: st.Mode})
	g.inodeCache.Upsert(name, st.Mode, ch)

	return ch, 0
}
