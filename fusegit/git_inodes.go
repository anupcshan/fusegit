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
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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
	overlayRoot string
	repo        repository
	socketPath  []byte
}

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

	for _, dirent := range dirents {
		if dirent.Type().IsRegular() {
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

	fh = fs.NewLoopbackFile(fd)

	of := &overlayFile{treeCtx: g.treeCtx, relativePath: fullRelativePath, fileHandle: fh}
	ch := g.NewPersistentInode(ctx, of, fs.StableAttr{Mode: st.Mode, Ino: st.Ino})

	g.inodeCache.Upsert(name, mode, ch)

	return ch, fh, 0, 0
}

type overlayFile struct {
	fs.Inode

	treeCtx      *gitTreeContext
	relativePath string
	fileHandle   fs.FileHandle
}

func (g *overlayFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return g.fileHandle.(fs.FileSetattrer).Setattr(ctx, in, out)
}

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

func printTimeSince(action string, start time.Time) {
	if false {
		log.Printf("Completed %s in %s", action, time.Since(start))
	}
}
