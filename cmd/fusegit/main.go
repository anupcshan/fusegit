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

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/hanwen/go-fuse/v2/fs"
	"google.golang.org/grpc"

	"github.com/anupcshan/fusegit/fg_proto"
	"github.com/anupcshan/fusegit/fusegit"
	"github.com/anupcshan/fusegit/processor"

	"net/http"
	_ "net/http/pprof"
)

var (
	debug = flag.Bool("debug", false, "print debug data")
)

func getRepoPaths(url, mountPoint string) (string, string, string, error) {
	cleanMount := path.Clean(mountPoint)
	hasher := sha256.New()
	hasher.Write([]byte(cleanMount))
	hasher.Write([]byte("\n"))
	hasher.Write([]byte(url))
	dirHash := hex.EncodeToString(hasher.Sum(nil))

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", "", "", err
	}

	cacheDir := path.Join(homeDir, ".cache", "fusegit", dirHash)
	cloneDir := path.Join(cacheDir, "git")
	if err := os.MkdirAll(cloneDir, 0755); err != nil {
		return "", "", "", err
	}

	overlayDir := path.Join(cacheDir, "write-overlay")
	if err := os.MkdirAll(overlayDir, 0755); err != nil {
		return "", "", "", err
	}

	return cloneDir, path.Join(cacheDir, "socket"), overlayDir, nil
}

func main() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatalf("Usage:\n %s repo-url MOUNTPOINT", os.Args[0])
	}

	if *debug {
		go func() {
			// Run a pprof instance in debug mode
			http.ListenAndServe(":6060", nil)
		}()
	}

	url := flag.Arg(0)
	mountPoint := flag.Arg(1)

	dir, socketPath, overlayRoot, err := getRepoPaths(url, mountPoint)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Initiated clone")

	var fsStorer storage.Storer
	fsStorer = filesystem.NewStorage(osfs.New(dir), cache.NewObjectLRUDefault())

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

	rootInode := fusegit.NewGitTreeInode(repo, socketPath, overlayRoot)

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatal(err)
	}

	proc, err := processor.NewFusegitProcessor(repo, rootInode)
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	fg_proto.RegisterFusegitServer(s, proc)

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
