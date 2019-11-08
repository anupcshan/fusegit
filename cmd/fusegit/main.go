package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"

	"net/http"
	_ "net/http/pprof"
)

var (
	debug = flag.Bool("debug", false, "print debug data")
)

type cacher interface {
	cacheAttrs() error
}

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

func (g *gitTreeInode) updateHash(treeHash plumbing.Hash) {
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
		if ent.Mode == filemode.Symlink {
			symlink := &gitSymlink{repo: g.repo, blobHash: ent.Hash}
			inode = g.NewInode(context.Background(), symlink, fs.StableAttr{Mode: uint32(ent.Mode)})
		} else if ent.Mode.IsFile() {
			file := &gitFile{repo: g.repo, blobHash: ent.Hash}
			inode = g.NewInode(context.Background(), file, fs.StableAttr{Mode: uint32(ent.Mode)})
		} else {
			dir := &gitTreeInode{repo: g.repo, treeHash: ent.Hash}
			inode = g.NewInode(context.Background(), dir, fs.StableAttr{Mode: syscall.S_IFDIR})
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

	if err := g.cacheAttrs(); err != nil {
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

	if err := g.cacheAttrs(); err != nil {
		return nil, syscall.EAGAIN
	}

	if ent, ok := g.lookupIndex[name]; ok {
		// cacherObj := ent.Operations().(cacher)
		if node, ok := ent.Operations().(fs.NodeGetattrer); ok {
			var attrOut fuse.AttrOut
			node.Getattr(ctx, nil, &attrOut)
			out.Attr = attrOut.Attr
		}

		// Caching Attr values that have not been produced by `Getattr` explicitly can be problematic.
		// Currently, we don't fetch `Attr` values for directories. If we see directory attr mismatches,
		// move SetAttrTimeout below `node.Getattr` just above.
		out.SetAttrTimeout(2 * time.Hour)
		out.SetEntryTimeout(2 * time.Hour)
		return ent, 0
	}

	return nil, syscall.ENOENT
}

type gitFile struct {
	fs.Inode

	mu         sync.Mutex
	repo       *git.Repository
	blobHash   plumbing.Hash
	cached     bool
	cachedObj  *object.Blob
	cachedData []byte
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
	out.SetTimeout(2 * time.Hour)
	return 0
}

func (f *gitFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if f.cacheAttrs() != nil {
		return nil, syscall.EAGAIN
	}
	defer printTimeSince("Reading file", time.Now())

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.cachedData == nil {
		r, err := f.cachedObj.Reader()
		if err != nil {
			log.Println("Error getting reader", err)
			return nil, syscall.EAGAIN
		}

		data, err := ioutil.ReadAll(r)
		if err != nil {
			log.Println("Error reading file", err)
			return nil, syscall.EAGAIN
		}

		f.cachedData = data
	}

	end := int(off) + len(dest)
	if end > len(f.cachedData) {
		end = len(f.cachedData)
	}
	return fuse.ReadResultData(f.cachedData[off:end]), 0
}

type gitSymlink struct {
	fs.Inode

	mu           sync.Mutex
	repo         *git.Repository
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

	obj, err := f.repo.BlobObject(f.blobHash)
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
	out.SetTimeout(2 * time.Hour)
	return 0
}

func (f *gitSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if f.cacheAttrs() != nil {
		return nil, syscall.EAGAIN
	}

	return f.cachedTarget, 0
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

	fsStorer := filesystem.NewStorage(osfs.New(dir), cache.NewObjectLRUDefault())
	repo, err := git.Clone(fsStorer, nil, &git.CloneOptions{
		URL: url,
	})
	if err != nil {
		if err != git.ErrRepositoryAlreadyExists {
			log.Fatal(err)
		}

		repo, err = git.Open(fsStorer, nil)
		if err != nil {
			log.Fatal(err)
		}

		if err := repo.Fetch(&git.FetchOptions{
			RemoteName: "origin",
			RefSpecs:   []config.RefSpec{"+refs/heads/*:refs/remotes/origin/*"},
		}); err != nil && err != git.NoErrAlreadyUpToDate {
			log.Fatal(err)
		}
	}

	log.Println("Completed clone")

	opts := &fs.Options{}
	opts.Debug = *debug
	opts.DisableXAttrs = true
	opts.UID = uint32(os.Getuid())
	opts.GID = uint32(os.Getgid())

	masterRef, err := repo.Reference("refs/remotes/origin/master", true)
	if err != nil {
		log.Fatal("Error locating origin/master")
	}

	log.Printf("Master ref %s", masterRef)

	headCommit, err := repo.CommitObject(masterRef.Hash())
	if err != nil {
		log.Fatal("Error identifying head commit")
	}

	tree, err := headCommit.Tree()
	if err != nil {
		log.Fatal("Error locating head tree")
	}

	rootInode := &gitTreeInode{repo: repo, treeHash: tree.Hash}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/checkout/") {
			revision := plumbing.NewHash(strings.TrimPrefix(r.URL.Path, "/checkout/"))
			commitObj, err := repo.CommitObject(revision)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "Unable to locate revision %s: %s", revision, err)
				return
			}
			log.Println("Checking out", revision)
			treeAtCommit, err := commitObj.Tree()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Unable to fetch tree for revision %s: %s", revision, err)
				return
			}

			rootInode.updateHash(treeAtCommit.Hash)
			w.Write([]byte("OK"))
		} else if strings.HasPrefix(r.URL.Path, "/commits/") {
			log.Println("Listing recent commits")
			commitIter, err := repo.Log(&git.LogOptions{From: masterRef.Hash()})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Error obtaining iterator %v", err)
				return
			}

			var commitShas []string
			for counter := 0; counter < 20; counter++ {
				commitObj, err := commitIter.Next()
				if err != nil && err != io.EOF {
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintf(w, "Error iterating commits %v", err)
					return
				}
				if err == io.EOF {
					break
				}
				commitShas = append(commitShas, commitObj.Hash.String())
			}
			enc := json.NewEncoder(w)
			enc.Encode(commitShas)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Unknown command %s", r.URL.Path)
		}
	})

	go http.ListenAndServe(":6060", nil)

	server, err := fs.Mount(mountPoint, rootInode, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
}
