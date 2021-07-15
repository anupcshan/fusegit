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

func (i *inodeIndex) Delete(name string) {
	if idx, found := i.index[name]; found {
		if idx.pos != len(i.dirents)-1 {
			// Move the last element over to the position that was deleted.
			lastElem := i.dirents[len(i.dirents)-1]
			i.dirents[idx.pos] = lastElem
			toUpdate := i.index[lastElem.Name]
			toUpdate.pos = idx.pos
			i.index[lastElem.Name] = toUpdate
		}

		i.dirents = i.dirents[:len(i.dirents)-1]
	}

	delete(i.index, name)
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
