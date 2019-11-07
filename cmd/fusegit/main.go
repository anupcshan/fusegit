package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"net/http"
	_ "net/http/pprof"
)

var (
	debug = flag.Bool("debug", false, "print debug data")
)

type gitTreeInode struct {
	fs.Inode

	mu       sync.Mutex
	repo     *git.Repository
	treeHash plumbing.Hash

	cached        bool
	cachedEntries []object.TreeEntry
	lookupIndex   map[string]*fs.Inode
	inodes        []*fs.Inode
}

func (g *gitTreeInode) scanTree() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cached {
		return nil
	}

	defer printTimeSince("Scan Tree", time.Now())

	tree, err := g.repo.TreeObject(g.treeHash)
	if err != nil {
		log.Printf("Error fetching tree object %s", g.treeHash)
		return io.ErrUnexpectedEOF
	}

	g.cached = true
	g.cachedEntries = tree.Entries
	g.lookupIndex = make(map[string]*fs.Inode, len(tree.Entries))
	for _, ent := range g.cachedEntries {
		var inode *fs.Inode
		if ent.Mode.IsFile() {
			inode = g.NewInode(context.Background(), &gitFile{repo: g.repo, blobHash: ent.Hash}, fs.StableAttr{Mode: uint32(ent.Mode)})
		} else {
			inode = g.NewInode(context.Background(), &gitTreeInode{repo: g.repo, treeHash: ent.Hash}, fs.StableAttr{Mode: syscall.S_IFDIR})
		}

		g.lookupIndex[ent.Name] = inode
	}

	return nil
}

func printTimeSince(action string, start time.Time) {
	if *debug {
		log.Printf("Completed %s in %s", action, time.Since(start))
	}
}

func (g *gitTreeInode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	defer printTimeSince("Readdir", time.Now())

	if err := g.scanTree(); err != nil {
		return nil, syscall.EAGAIN
	}

	result := make([]fuse.DirEntry, 0, len(g.cachedEntries))

	for _, ent := range g.cachedEntries {
		result = append(result, fuse.DirEntry{Name: ent.Name, Mode: uint32(ent.Mode)})
	}

	return fs.NewListDirStream(result), 0
}

func (g *gitTreeInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer printTimeSince("Lookup", time.Now())

	if err := g.scanTree(); err != nil {
		return nil, syscall.EAGAIN
	}

	if ent, ok := g.lookupIndex[name]; ok {
		return ent, 0
	}

	return nil, syscall.ENOENT
}

type gitFile struct {
	fs.Inode

	mu        sync.Mutex
	repo      *git.Repository
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

	obj, err := f.repo.BlobObject(f.blobHash)
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
	return 0
}

func (f *gitFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if f.cacheAttrs() != nil {
		return nil, syscall.EAGAIN
	}

	r, err := f.cachedObj.Reader()
	if err != nil {
		log.Println("Error getting reader", err)
		return nil, syscall.EAGAIN
	}

	_, err = io.CopyN(ioutil.Discard, r, off)
	if err != nil {
		log.Println("Error skipping bytes", err)
		return nil, syscall.EAGAIN
	}

	n, err := r.Read(dest)
	if err != nil && err != io.EOF {
		log.Println("Error reading file", err, f.blobHash)
		return nil, syscall.EAGAIN
	}

	return fuse.ReadResultData(dest[:n]), 0
}

func getCloneDir(url, mountPoint string) (string, error) {
	cleanMount := path.Clean(mountPoint)
	hasher := sha256.New()
	hasher.Write([]byte(cleanMount))
	hasher.Write([]byte("\n"))
	hasher.Write([]byte(url))
	dirHash := hex.EncodeToString(hasher.Sum(nil))

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	cloneDir := path.Join(homeDir, ".cache", "fusegit", dirHash)
	if err := os.MkdirAll(cloneDir, 0755); err != nil {
		return "", err
	}

	return cloneDir, nil
}

func main() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatalf("Usage:\n %s repo-url MOUNTPOINT", os.Args[0])
	}

	url := flag.Arg(0)
	mountPoint := flag.Arg(1)

	dir, err := getCloneDir(url, mountPoint)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Initiated clone")

	repo, err := git.PlainClone(dir, true, &git.CloneOptions{
		URL:   url,
		Depth: 1,
	})
	if err != nil {
		if err != git.ErrRepositoryAlreadyExists {
			log.Fatal(err)
		}

		repo, err = git.PlainOpen(dir)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Completed clone")

	opts := &fs.Options{}
	opts.Debug = *debug
	opts.DisableXAttrs = true

	headRef, err := repo.Head()
	if err != nil {
		log.Fatal("Error locating HEAD")
	}

	headCommit, err := repo.CommitObject(headRef.Hash())
	if err != nil {
		log.Fatal("Error identifying head commit")
	}
	tree, err := headCommit.Tree()
	if err != nil {
		log.Fatal("Error locating head tree")
	}

	go http.ListenAndServe(":6060", nil)

	server, err := fs.Mount(mountPoint, &gitTreeInode{repo: repo, treeHash: tree.Hash}, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
}
