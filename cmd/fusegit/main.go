package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"gopkg.in/src-d/go-git.v4"
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
		log.Println(ent)
		if ent.Mode.IsFile() {
			ch := ino.NewInode(context.Background(), &fs.Inode{}, fs.StableAttr{})
			ino.AddChild(ent.Name, ch, true)
		} else {
			ch := ino.NewInode(context.Background(), &fs.Inode{}, fs.StableAttr{Mode: syscall.S_IFDIR})
			ino.AddChild(ent.Name, ch, true)
		}
	}
}

func main() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	debug := flag.Bool("debug", false, "print debug data")
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
