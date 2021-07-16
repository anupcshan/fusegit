package fusegit

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const tombstonePrefix = ".__FUSEGIT_TOMBSTONE__"
const ignoreUnderlayFlagFilename = ".__FUSEGIT_IGNOREUNDERLAY__"

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

var _ fs.NodeMkdirer = (*gitTreeInode)(nil)
var _ fs.NodeRenamer = (*gitTreeInode)(nil)
var _ fs.NodeRmdirer = (*gitTreeInode)(nil)
var _ fs.NodeSymlinker = (*gitTreeInode)(nil)
var _ fs.NodeUnlinker = (*gitTreeInode)(nil)

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

	// Ordering of tombstone and non-tombstone entries matter. Always process tombstone/ignore underlay markers first. These never apply to overlay contents.
	// Once marker files are processed, handle all regular overlay entities.

	var ignoreUnderlay bool
	var files, directories []os.DirEntry

	for _, dirent := range dirents {
		// In the initial pass, handle all tombstone files and ignore underlay markers.
		if strings.HasPrefix(dirent.Name(), tombstonePrefix) {
			name := dirent.Name()[len(tombstonePrefix):]
			g.inodeCache.Delete(name)
			g.RmChild(name)
		} else if dirent.Name() == ignoreUnderlayFlagFilename {
			ignoreUnderlay = true
		} else if dirent.Type() != os.ModeDir {
			files = append(files, dirent)
		} else {
			directories = append(directories, dirent)
		}
	}

	if ignoreUnderlay {
		for _, dirent := range g.inodeCache.Dirents() {
			g.inodeCache.Delete(dirent.Name)
			g.RmChild(dirent.Name)
		}
	}

	for _, dirent := range files {
		if dirent.Type().IsRegular() {
			name := dirent.Name()

			g.inodeCache.Delete(name)
			g.RmChild(name)

			of := &overlayFile{treeCtx: g.treeCtx, relativePath: filepath.Join(g.Path(nil), name)}
			mode := uint32(dirent.Type())
			ch := g.NewPersistentInode(context.Background(), of, fs.StableAttr{Mode: mode})
			g.inodeCache.Upsert(name, mode, ch)
		} else if dirent.Type() == os.ModeSymlink {
			name := dirent.Name()

			g.inodeCache.Delete(name)
			g.RmChild(name)

			of := &overlaySymlink{treeCtx: g.treeCtx, relativePath: filepath.Join(g.Path(nil), name)}
			fInfo, err := dirent.Info()
			if err != nil {
				// TODO: Maybe don't fail here. This is recoverable.
				return err
			}
			mode := uint32(fInfo.Mode().Perm() | syscall.S_IFLNK)
			ch := g.NewPersistentInode(context.Background(), of, fs.StableAttr{Mode: mode})
			g.inodeCache.Upsert(name, mode, ch)
		}
	}

	for _, dirent := range directories {
		name := dirent.Name()

		// If we encounter a directory in the overlay, first check if a node with the same name exists in the underlay/git tree.
		// If yes, don't bother changing anything. If the underlay node is a directory, it will correctly pick up everything in that directory.
		// If this directory doesn't exist in the underlay, it should be treated as a new directory node.

		if g.inodeCache.LookupInode(name) != nil {
			// Already exists. Ignore and keep going
			continue
		}

		dir := &gitTreeInode{treeCtx: g.treeCtx}
		inode := g.NewPersistentInode(context.Background(), dir, fs.StableAttr{Mode: syscall.S_IFDIR})
		g.inodeCache.Upsert(name, syscall.S_IFDIR, inode)
	}

	return nil
}

func (g *gitTreeInode) scanTree() error {
	defer printTimeSince("Scan Tree", time.Now())

	if g.treeHash == plumbing.ZeroHash {
		// We either have an actual git hash with all zeros or this signifies an overlay-only directory.
		// We're going to assume its an overlay-only directory.
		return nil
	}

	tree, err := g.treeCtx.repo.TreeObject(g.treeHash)
	if err != nil {
		log.Printf("Error fetching tree object %s", g.treeHash)
		return io.ErrUnexpectedEOF
	}

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
			file := &gitFile{treeCtx: g.treeCtx, blobHash: ent.Hash, name: ent.Name}
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

	g.inodeCache = NewInodeCache()

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

	_, parentInode := g.Parent()
	parentTreeInode := parentInode.Operations().(*gitTreeInode)

	if err := parentTreeInode.replicateTreeInOverlay(); err != nil {
		return err
	}

	if err := syscall.Mkdir(filepath.Join(g.treeCtx.overlayRoot, g.Path(nil)), 0775); err != nil {
		log.Println("Error creating dir", err)
		return err
	}

	g.isInitializedInOverlay = true

	return nil
}

func (g *gitTreeInode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// TODO: Clear any existing tombstone records on success.
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
	out.SetAttrTimeout(DefaultCacheTimeout)
	out.SetEntryTimeout(DefaultCacheTimeout)

	of := &overlayFile{treeCtx: g.treeCtx, relativePath: fullRelativePath}
	ch := g.NewPersistentInode(ctx, of, fs.StableAttr{Mode: st.Mode})
	g.inodeCache.Upsert(name, mode, ch)

	return ch, fs.NewLoopbackFile(fd), 0, 0
}

func (g *gitTreeInode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// TODO: Clear any existing tombstone records on success.
	fullRelativePath := filepath.Join(g.Path(nil), name)
	fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)

	if err := syscall.Symlink(target, fullOverlayPath); err != nil {
		return nil, err.(syscall.Errno)
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(fullOverlayPath, &st); err != nil {
		syscall.Unlink(fullOverlayPath)
		return nil, err.(syscall.Errno)
	}
	out.FromStat(&st)

	of := &overlaySymlink{treeCtx: g.treeCtx, relativePath: fullRelativePath}
	ch := g.NewPersistentInode(ctx, of, fs.StableAttr{Mode: st.Mode})
	g.inodeCache.Upsert(name, st.Mode, ch)

	return ch, 0
}

func (g *gitTreeInode) Unlink(ctx context.Context, name string) syscall.Errno {
	switch g.inodeCache.LookupInode(name).Operations().(type) {
	case *gitFile, *gitSymlink:
		if err := g.installTombstone(ctx, name); err != nil {
			return err.(syscall.Errno)
		}

	case *overlayFile, *overlaySymlink:
		if err := g.replicateTreeInOverlay(); err != nil {
			return err.(syscall.Errno)
		}

		fullRelativePath := filepath.Join(g.Path(nil), name)
		fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)
		if err := syscall.Unlink(fullOverlayPath); err != nil {
			return err.(syscall.Errno)
		}
	}

	g.inodeCache.Delete(name)
	g.RmChild(name)
	return 0
}

func (g *gitTreeInode) Rmdir(ctx context.Context, name string) syscall.Errno {
	switch g.inodeCache.LookupInode(name).Operations().(type) {
	case *gitTreeInode:
		if err := g.installTombstone(ctx, name); err != nil {
			return err.(syscall.Errno)
		}

		// Nuke anything that exists in the overlay directory.
		fullRelativePath := filepath.Join(g.Path(nil), name)
		fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)
		if err := os.RemoveAll(fullOverlayPath); err != nil {
			return syscall.EIO
		}
	}

	g.inodeCache.Delete(name)
	g.RmChild(name)
	return 0
}

func (g *gitTreeInode) installTombstone(ctx context.Context, name string) error {
	if err := g.replicateTreeInOverlay(); err != nil {
		return err.(syscall.Errno)
	}

	fullRelativePath := filepath.Join(g.Path(nil), tombstonePrefix+name)
	fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)
	fd, err := syscall.Creat(fullOverlayPath, 0550)
	if err != nil {
		return err
	}
	syscall.Close(fd)

	return nil
}

func (g *gitTreeInode) removeTombstone(ctx context.Context, name string) error {
	fullRelativePath := filepath.Join(g.Path(nil), tombstonePrefix+name)
	fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)
	return syscall.Unlink(fullOverlayPath)
}

func (g *gitTreeInode) emitIgnoreUnderlayMarker(ctx context.Context, name string) error {
	fullRelativePath := filepath.Join(g.Path(nil), name, ignoreUnderlayFlagFilename)
	fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)
	fd, err := syscall.Creat(fullOverlayPath, 0550)
	if err != nil {
		return err
	}
	syscall.Close(fd)

	return nil
}

func (g *gitTreeInode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// TODO: There are some error handling gotchas here.
	// For example, removing the tombstone and then failing to mkdir will make any underlay directories to reappear. Figure out the right way to order these.
	g.removeTombstone(ctx, name)

	if err := g.replicateTreeInOverlay(); err != nil {
		return nil, err.(syscall.Errno)
	}

	fullRelativePath := filepath.Join(g.Path(nil), name)
	fullOverlayPath := filepath.Join(g.treeCtx.overlayRoot, fullRelativePath)
	if err := syscall.Mkdir(fullOverlayPath, mode); err != nil && err.(syscall.Errno) != syscall.EEXIST {
		// Its OK for syscall to return EEXIST in case when we are mkdir'ing a previously deleted directory which has some tombstone files in them.
		return nil, err.(syscall.Errno)
	}

	if err := g.emitIgnoreUnderlayMarker(ctx, name); err != nil {
		// TODO: Cleanup
		return nil, err.(syscall.Errno)
	}

	dir := &gitTreeInode{treeCtx: g.treeCtx}
	inode := g.NewPersistentInode(context.Background(), dir, fs.StableAttr{Mode: syscall.S_IFDIR})
	g.inodeCache.Upsert(name, mode, inode)

	return inode, 0
}

func (g *gitTreeInode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	targetDirGitInode := newParent.EmbeddedInode().Operations().(*gitTreeInode)

	inode := g.inodeCache.LookupInode(name)
	ops := inode.Operations()
	switch ops.(type) {
	case *gitFile:
		if err := ops.(*gitFile).ensureReplicated(); err != nil {
			return err.(syscall.Errno)
		}

		// TODO: Handle gitSymlink
	}

	g.RmChild(name)
	g.inodeCache.Delete(name)
	fullSrcOverlayPath := filepath.Join(g.treeCtx.overlayRoot, g.Path(nil), name)

	fullTargetOverlayPath := filepath.Join(g.treeCtx.overlayRoot, targetDirGitInode.Path(nil), newName)

	// Ensure any target entry which might be getting overwritten is nuked
	targetDirGitInode.inodeCache.Delete(newName)
	targetDirGitInode.inodeCache.Upsert(newName, 0100664, newParent.EmbeddedInode().NewPersistentInode(ctx, inode.Operations(), fs.StableAttr{Mode: 0100664}))

	err := syscall.Rename(fullSrcOverlayPath, fullTargetOverlayPath)
	if err != nil {
		return err.(syscall.Errno)
	}
	return 0
}
