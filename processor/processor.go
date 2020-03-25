package processor

import (
	"context"
	"log"
	"sync"

	"github.com/anupcshan/fusegit/fg_proto"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type updater interface {
	UpdateHash(plumbing.Hash)
}

type fusegitProcessor struct {
	head *object.Commit
	repo *git.Repository
	root updater

	lock sync.Mutex
}

func (f *fusegitProcessor) Checkout(_ context.Context, req *fg_proto.CheckoutRequest) (*fg_proto.CheckoutResponse, error) {
	revision := plumbing.NewHash(req.GetRevisionHash())
	commitObj, err := f.repo.CommitObject(revision)

	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Couldn't locate revision %s: %s", revision, err)
	}

	log.Println("Checking out", revision)
	treeAtCommit, err := commitObj.Tree()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Unable to fetch tree for revision %s: %s", revision, err)
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.root.UpdateHash(treeAtCommit.Hash)
	f.head = commitObj
	return &fg_proto.CheckoutResponse{}, nil
}

func (f *fusegitProcessor) Fetch(_ context.Context, _ *fg_proto.FetchRequest) (*fg_proto.FetchResponse, error) {
	err := f.repo.Fetch(&git.FetchOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{"+refs/heads/*:refs/remotes/origin/*"},
	})

	if err != nil && err != git.NoErrAlreadyUpToDate {
		return nil, err
	}

	return &fg_proto.FetchResponse{}, nil
}

func (f *fusegitProcessor) Status(_ context.Context, _ *fg_proto.StatusRequest) (*fg_proto.StatusResponse, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	return &fg_proto.StatusResponse{
		RevisionHash: f.head.Hash.String(),
	}, nil
}

func NewFusegitProcessor(head *object.Commit, repo *git.Repository, root updater) fg_proto.FusegitServer {
	return &fusegitProcessor{
		head: head,
		repo: repo,
		root: root,
	}
}
