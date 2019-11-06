package main

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

var (
	debug = flag.Bool("debug", false, "print debug data")
)

type gitFSRoot struct {
	fs.Inode

	repo *git.Repository
}

var _ = (fs.NodeOnAdder)((*gitFSRoot)(nil))

func (g *gitFSRoot) OnAdd(ctx context.Context) {
	log.Println("OnAdd called")
	headRef, err := g.repo.Head()
	if err != nil {
		log.Println("Error locating HEAD")
		return
	}
	headCommit, err := g.repo.CommitObject(headRef.Hash())
	if err != nil {
		log.Println("Error identifying head commit")
		return
	}
	tree, err := headCommit.Tree()
	if err != nil {
		log.Println("Error locating head tree")
	}

	ino := g.EmbeddedInode()
	for _, ent := range tree.Entries {
		if ent.Mode.IsFile() {
			obj, err := g.repo.BlobObject(ent.Hash)
			if err != nil {
				log.Printf("Error locating blob object for %s", ent.Name)
				continue
			}
			reader, err := obj.Reader()
			if err != nil {
				log.Printf("Error fetching reader for blob object for %s", ent.Name)
				continue
			}
			contents, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Printf("Error reading blob contents for %s", ent.Name)
				continue
			}
			ch := ino.NewInode(context.Background(), &fs.MemRegularFile{
				Data: contents,
			}, fs.StableAttr{})
			ino.AddChild(ent.Name, ch, true)
		} else {
			ch := ino.NewInode(context.Background(), &gitTreeInode{repo: g.repo, treeHash: ent.Hash}, fs.StableAttr{Mode: syscall.S_IFDIR})
			ino.AddChild(ent.Name, ch, true)
		}
	}
}

type gitTreeInode struct {
	fs.Inode

	mu            sync.Mutex
	repo          *git.Repository
	treeHash      plumbing.Hash
	cached        bool
	cachedEntries []object.TreeEntry
}

func (g *gitTreeInode) scanTree() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cached {
		return nil
	}

	tree, err := g.repo.TreeObject(g.treeHash)
	if err != nil {
		log.Printf("Error fetching tree object %s", g.treeHash)
		return io.ErrUnexpectedEOF
	}

	g.cached = true
	g.cachedEntries = tree.Entries
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

	for _, ent := range g.cachedEntries {
		if ent.Name != name {
			continue
		}

		if ent.Mode.IsFile() {
			obj, err := g.repo.BlobObject(ent.Hash)
			if err != nil {
				log.Printf("Error locating blob object for %s", ent.Name)
				return nil, syscall.EAGAIN
			}
			reader, err := obj.Reader()
			if err != nil {
				log.Printf("Error fetching reader for blob object for %s", ent.Name)
				return nil, syscall.EAGAIN
			}
			contents, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Printf("Error reading blob contents for %s", ent.Name)
				return nil, syscall.EAGAIN
			}
			return g.NewInode(context.Background(), &fs.MemRegularFile{
				Data: contents,
			}, fs.StableAttr{}), 0
		} else {
			return g.NewInode(context.Background(), &gitTreeInode{repo: g.repo, treeHash: ent.Hash}, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
		}
	}

	return nil, syscall.ENOENT
}

func main() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatalf("Usage:\n %s repo-url MOUNTPOINT", os.Args[0])
	}
	dir, err := ioutil.TempDir("", "clone-example")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Initiated clone")

	repo, err := git.PlainClone(dir, true, &git.CloneOptions{
		URL:   flag.Arg(0),
		Depth: 1,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Completed clone")

	opts := &fs.Options{}
	opts.Debug = *debug
	server, err := fs.Mount(flag.Arg(1), &gitFSRoot{repo: repo}, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()

	_ = os.RemoveAll(dir)
}
