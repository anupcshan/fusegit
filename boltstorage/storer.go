package boltstorage

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"log"

	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"

	bolt "go.etcd.io/bbolt"
)

type boltStorage struct {
	db *bolt.DB
}

var configBucket = []byte("config")
var indexBucket = []byte("index")
var objectsBucket = []byte("objects")
var referencesBucket = []byte("references")

func NewBoltStorage(db *bolt.DB) (*boltStorage, error) {
	log.Println("Opening bolt storage db")
	if tx, err := db.Begin(true); err != nil {
		return nil, err
	} else {
		if _, err := tx.CreateBucketIfNotExists(configBucket); err != nil {
			return nil, err
		}

		if _, err := tx.CreateBucketIfNotExists(indexBucket); err != nil {
			return nil, err
		}

		if _, err := tx.CreateBucketIfNotExists(objectsBucket); err != nil {
			return nil, err
		}

		if _, err := tx.CreateBucketIfNotExists(referencesBucket); err != nil {
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return &boltStorage{
		db: db,
	}, nil
}

func (s *boltStorage) CheckAndSetReference(nu, old *plumbing.Reference) error {
	if old != nil {
		return s.db.Update(func(tx *bolt.Tx) error {
			refB := tx.Bucket(referencesBucket)
			oldValue := refB.Get([]byte(old.Name()))
			if string(oldValue) != old.Hash().String() {
				return fmt.Errorf("Mismatch in old and new values")
			}

			return refB.Put([]byte(nu.Name()), []byte(nu.Hash().String()))
		})
	}

	return s.SetReference(nu)
}

func (s *boltStorage) SetReference(ref *plumbing.Reference) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		refB := tx.Bucket(referencesBucket)
		return refB.Put([]byte(ref.Name()), []byte(ref.Hash().String()))
	})
}

func (s *boltStorage) Config() (*config.Config, error) {
	conf := new(config.Config)
	err := s.db.View(func(tx *bolt.Tx) error {
		confB := tx.Bucket(configBucket)
		val := confB.Get([]byte("foo"))
		if val == nil {
			conf = config.NewConfig()
			return nil
		}

		return gob.NewDecoder(bytes.NewReader(val)).Decode(conf)
	})

	if err != nil {
		return nil, err
	}

	return conf, nil
}

func (s *boltStorage) SetConfig(conf *config.Config) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		confB := tx.Bucket(configBucket)
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(conf); err != nil {
			return err
		}
		return confB.Put([]byte("foo"), buf.Bytes())
	})
}

func (s *boltStorage) CountLooseRefs() (int, error) {
	var refs int
	err := s.db.View(func(tx *bolt.Tx) error {
		refB := tx.Bucket(referencesBucket)
		return refB.ForEach(func(_, _ []byte) error {
			refs++
			return nil
		})
	})

	return refs, err
}

func (s *boltStorage) EncodedObject(otype plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	obj := new(plumbing.MemoryObject)
	err := s.db.View(func(tx *bolt.Tx) error {
		objB := tx.Bucket(objectsBucket)
		value := objB.Get([]byte(hash.String()))
		if value == nil || len(value) < 1 {
			return plumbing.ErrObjectNotFound
		}

		obj.SetType(plumbing.ObjectType(value[0]))
		reader, err := gzip.NewReader(bytes.NewReader(value[1:]))
		if err != nil {
			return err
		}
		_, err = io.Copy(obj, reader)
		return err
	})

	if err != nil {
		return nil, err
	}

	if otype != plumbing.AnyObject && otype != obj.Type() {
		return nil, plumbing.ErrObjectNotFound
	}

	return obj, nil
}

func (s *boltStorage) SetEncodedObject(encodedObj plumbing.EncodedObject) (plumbing.Hash, error) {
	return encodedObj.Hash(), s.db.Update(func(tx *bolt.Tx) error {
		objB := tx.Bucket(objectsBucket)
		buf := bytes.NewBuffer(make([]byte, 0, encodedObj.Size()))
		buf.WriteByte(byte(encodedObj.Type()))
		reader, err := encodedObj.Reader()
		if err != nil {
			return err
		}
		writer, err := gzip.NewWriterLevel(buf, gzip.BestSpeed)
		if err != nil {
			return err
		}
		if _, err := io.Copy(writer, reader); err != nil {
			return err
		}

		if err := writer.Close(); err != nil {
			return err
		}

		return objB.Put([]byte(encodedObj.Hash().String()), buf.Bytes())
	})
}

func (s *boltStorage) EncodedObjectSize(hash plumbing.Hash) (int64, error) {
	obj, err := s.EncodedObject(plumbing.AnyObject, hash)
	if err != nil {
		return 0, err
	}
	return obj.Size(), nil
}

func (s *boltStorage) HasEncodedObject(hash plumbing.Hash) error {
	// NOTE: There are definitely more efficient ways to do this
	_, err := s.EncodedObject(plumbing.AnyObject, hash)
	return err
}

func (s *boltStorage) Index() (*index.Index, error) {
	var idx index.Index
	err := s.db.View(func(tx *bolt.Tx) error {
		idxB := tx.Bucket(indexBucket)
		val := idxB.Get([]byte("foo"))
		if val == nil {
			return fmt.Errorf("No index found")
		}

		return gob.NewDecoder(bytes.NewReader(val)).Decode(&idx)
	})

	if err != nil {
		return nil, err
	}

	return &idx, nil
}

func (s *boltStorage) SetIndex(idx *index.Index) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		idxB := tx.Bucket(indexBucket)
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(idx); err != nil {
			return err
		}
		return idxB.Put([]byte("foo"), buf.Bytes())
	})
}

func (s *boltStorage) IterEncodedObjects(otype plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	return nil, fmt.Errorf("IterEncodedObjects not implemented")
}

func (s *boltStorage) IterReferences() (storer.ReferenceIter, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var refs []*plumbing.Reference

	refB := tx.Bucket(referencesBucket)
	if err := refB.ForEach(func(k, v []byte) error {
		refs = append(refs, plumbing.NewReferenceFromStrings(string(k), string(v)))
		return nil
	}); err != nil {
		return nil, err
	}

	return storer.NewReferenceSliceIter(refs), nil
}

func (s *boltStorage) Module(string) (storage.Storer, error) {
	return nil, fmt.Errorf("Don't use modules")
}

func (s *boltStorage) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{}
}

func (s *boltStorage) PackRefs() error {
	return fmt.Errorf("PackRefs not supported")
}

func (s *boltStorage) Reference(refName plumbing.ReferenceName) (*plumbing.Reference, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	refB := tx.Bucket(referencesBucket)
	val := refB.Get([]byte(refName))
	if val == nil {
		return nil, plumbing.ErrReferenceNotFound
	}
	return plumbing.NewHashReference(refName, plumbing.NewHash(string(val))), nil
}

func (s *boltStorage) RemoveReference(refName plumbing.ReferenceName) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		refB := tx.Bucket(referencesBucket)
		return refB.Delete([]byte(refName))
	})
}

func (s *boltStorage) SetShallow([]plumbing.Hash) error {
	return fmt.Errorf("SetShallow not implemented")
}

func (s *boltStorage) Shallow() ([]plumbing.Hash, error) {
	return nil, fmt.Errorf("Shallow not implemented")
}

var _ = (storage.Storer)((*boltStorage)(nil))
