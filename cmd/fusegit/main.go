package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/hanwen/go-fuse/v2/fs"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"

	"github.com/anupcshan/fusegit/fusegit"

	"net/http"
	_ "net/http/pprof"
)

var (
	debug = flag.Bool("debug", false, "print debug data")
)

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

	rootInode := fusegit.NewGitTreeInode(repo.Storer, tree.Hash)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/checkout/") {
			revision := plumbing.NewHash(strings.TrimPrefix(r.URL.Path, "/checkout/"))
			commitObj, err := repo.CommitObject(revision)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "Unable to locate revision %s: %s", revision, err)
				return
			}
			headCommit = commitObj
			log.Println("Checking out", revision)
			treeAtCommit, err := commitObj.Tree()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Unable to fetch tree for revision %s: %s", revision, err)
				return
			}

			rootInode.UpdateHash(treeAtCommit.Hash)
			w.Write([]byte("OK\n"))
		} else if strings.HasPrefix(r.URL.Path, "/fetch/") {
			if err := repo.Fetch(&git.FetchOptions{
				RemoteName: "origin",
				RefSpecs:   []config.RefSpec{"+refs/heads/*:refs/remotes/origin/*"},
			}); err != nil && err != git.NoErrAlreadyUpToDate {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Unable to fetch: %s", err)
				return
			}

			w.Write([]byte("OK\n"))
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
		} else if strings.HasPrefix(r.URL.Path, "/status/") {
			log.Println("Current commit is", headCommit.Hash)
			fmt.Fprintf(w, "%s\n", headCommit.Hash)
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
