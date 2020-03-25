package main

//go:generate protoc -I ../../fg_proto ../../fg_proto/fusegit.proto --go_out=plugins=grpc:../../fg_proto

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"log"
	"net"
	"os"
	"path"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/hanwen/go-fuse/v2/fs"
	"google.golang.org/grpc"

	"github.com/anupcshan/fusegit/boltstorage"
	"github.com/anupcshan/fusegit/fg_proto"
	"github.com/anupcshan/fusegit/fusegit"
	"github.com/anupcshan/fusegit/processor"

	bolt "go.etcd.io/bbolt"
)

var (
	debug   = flag.Bool("debug", false, "print debug data")
	useBolt = flag.Bool("use-bolt", false, "Use BoltDB to store git info instead of .git directory")
)

func getRepoPaths(url, mountPoint string) (string, string, error) {
	cleanMount := path.Clean(mountPoint)
	hasher := sha256.New()
	hasher.Write([]byte(cleanMount))
	hasher.Write([]byte("\n"))
	hasher.Write([]byte(url))
	dirHash := hex.EncodeToString(hasher.Sum(nil))

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", "", err
	}

	cacheDir := path.Join(homeDir, ".cache", "fusegit")
	cloneDir := path.Join(cacheDir, dirHash)
	if err := os.MkdirAll(cloneDir, 0755); err != nil {
		return "", "", err
	}

	return cloneDir, path.Join(cacheDir, dirHash+".socket"), nil
}

func main() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatalf("Usage:\n %s repo-url MOUNTPOINT", os.Args[0])
	}

	url := flag.Arg(0)
	mountPoint := flag.Arg(1)

	dir, socketPath, err := getRepoPaths(url, mountPoint)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Initiated clone")

	var fsStorer storage.Storer
	if !*useBolt {
		fsStorer = filesystem.NewStorage(osfs.New(dir), cache.NewObjectLRUDefault())
	} else {
		db, err := bolt.Open(path.Join(dir, "git.db"), 0600, &bolt.Options{
			Timeout: 2 * time.Second,
			NoSync:  true,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		fsStorer, err = boltstorage.NewBoltStorage(db)
		if err != nil {
			log.Fatal(err)
		}
	}

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

	rootInode := fusegit.NewGitTreeInode(repo.Storer, tree.Hash, socketPath)

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	fg_proto.RegisterFusegitServer(
		s,
		processor.NewFusegitProcessor(headCommit, repo, rootInode),
	)

	go func() {
		s.Serve(l)
	}()

	server, err := fs.Mount(mountPoint, rootInode, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
	l.Close()
}
