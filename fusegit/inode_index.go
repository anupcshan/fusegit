package fusegit

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type indexedEntry struct {
	pos   int
	inode *fs.Inode
}

type inodeIndex struct {
	dirents []fuse.DirEntry
	index   map[string]indexedEntry
}

func NewInodeCache() *inodeIndex {
	return &inodeIndex{
		dirents: []fuse.DirEntry{},
		index:   map[string]indexedEntry{},
	}
}

func (i *inodeIndex) Upsert(name string, mode uint32, inode *fs.Inode) {
	dirent := fuse.DirEntry{Name: name, Mode: mode}
	if idx, found := i.index[dirent.Name]; found {
		i.dirents[idx.pos] = dirent
	}

	idx := len(i.index)
	i.index[dirent.Name] = indexedEntry{pos: idx, inode: inode}
	i.dirents = append(i.dirents, dirent)
}

func (i *inodeIndex) Dirents() []fuse.DirEntry {
	return i.dirents
}

func (i *inodeIndex) LookupInode(name string) *fs.Inode {
	entry, ok := i.index[name]
	if !ok {
		return nil
	}
	return entry.inode
}
