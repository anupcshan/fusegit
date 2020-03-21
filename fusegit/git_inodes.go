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
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Default time we want an entry to be cached in the kernel.
// In ~ all cases so far, this number should be as high as we can possibly set. We should be
// explicilty pushing invalidation notices to the kernel for entries when they change.
const DefaultCacheTimeout = 300 * time.Hour

type gitTreeInode struct {
	fs.Inode

	mu       sync.Mutex
	storer   storage.Storer
	treeHash plumbing.Hash

	cached        bool
	cachedEntries []object.TreeEntry
	lookupIndex   map[string]*fs.Inode
	inodes        []*fs.Inode
}

func NewGitTreeInode(storer storage.Storer, treeHash plumbing.Hash) *gitTreeInode {
	return &gitTreeInode{
		storer:   storer,
		treeHash: treeHash,
	}
}

func (g *gitTreeInode) UpdateHash(treeHash plumbing.Hash) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cached {
		for k := range g.lookupIndex {
			g.NotifyEntry(k)
		}
	}

	g.treeHash = treeHash
	g.cached = false
	g.RmAllChildren()
}

func (g *gitTreeInode) cacheAttrs() error {
	if g.cached {
		return nil
	}

	defer printTimeSince("Scan Tree", time.Now())

	tree, err := object.GetTree(g.storer, g.treeHash)
	if err != nil {
		log.Printf("Error fetching tree object %s", g.treeHash)
		return io.ErrUnexpectedEOF
	}

	g.cached = true
	g.cachedEntries = tree.Entries
	g.lookupIndex = make(map[string]*fs.Inode, len(tree.Entries))
	for _, ent := range g.cachedEntries {
		var inode *fs.Inode
		if ent.Mode == filemode.Symlink {
			symlink := &gitSymlink{storer: g.storer, blobHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), symlink, fs.StableAttr{Mode: uint32(ent.Mode)})
		} else if ent.Mode.IsFile() {
			file := &gitFile{storer: g.storer, blobHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), file, fs.StableAttr{Mode: uint32(ent.Mode)})
		} else if ent.Mode == filemode.Submodule {
			dir := &gitTreeInode{storer: g.storer, treeHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), dir, fs.StableAttr{Mode: syscall.S_IFDIR})
		} else {
			dir := &gitTreeInode{storer: g.storer, treeHash: ent.Hash}
			inode = g.NewPersistentInode(context.Background(), dir, fs.StableAttr{Mode: syscall.S_IFDIR})
		}

		g.lookupIndex[ent.Name] = inode
	}

	return nil
}

func (g *gitTreeInode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	defer printTimeSince("Readdir", time.Now())

	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.cacheAttrs(); err != nil {
		return nil, syscall.EAGAIN
	}

	result := make([]fuse.DirEntry, 0, len(g.cachedEntries))

	for _, ent := range g.cachedEntries {
		var mode uint32
		if ent.Mode == filemode.Submodule {
			mode = syscall.S_IFDIR
		} else {
			mode = uint32(ent.Mode)
		}
		result = append(result, fuse.DirEntry{Name: ent.Name, Mode: mode})
	}

	return fs.NewListDirStream(result), 0
}

func (g *gitTreeInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer printTimeSince("Lookup", time.Now())

	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.cacheAttrs(); err != nil {
		return nil, syscall.EAGAIN
	}

	if ent, ok := g.lookupIndex[name]; ok {
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

type gitFile struct {
	fs.Inode

	mu        sync.Mutex
	storer    storage.Storer
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

	obj, err := object.GetBlob(f.storer, f.blobHash)
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
	storer       storage.Storer
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

	obj, err := object.GetBlob(f.storer, f.blobHash)
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
