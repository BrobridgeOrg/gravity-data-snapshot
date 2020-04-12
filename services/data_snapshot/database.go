package data_snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	pb "gravity-data-snapshot/pb"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Database struct {
	name string
	db   *leveldb.DB
}

func OpenDatabase(dbname string) *Database {

	dbpath := fmt.Sprintf("%s/%s", viper.GetString("database.dbpath"), dbname)

	// Open database
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		log.Error(err)
		return nil
	}

	return &Database{
		name: dbname,
		db:   db,
	}
}

func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil

}
func Uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(n))

	return b
}

func BytesToUint64(data []byte) uint64 {
	return uint64(binary.LittleEndian.Uint64(data))
}

func (database *Database) ProcessData(sequence uint64, projection *Projection) error {

	var primaryKey []byte
	hasPrimary := false
	for _, field := range projection.Fields {
		if field.Primary == true {
			key, err := GetBytes(field.Value)
			if err != nil {
				return err
			}

			primaryKey = key
			hasPrimary = true
		}
	}

	// Add prefix
	primaryKey = bytes.Join([][]byte{[]byte("key"), primaryKey}, []byte("-"))

	if projection.Method == "delete" {
		return database.DeleteRecord(sequence, primaryKey)
	}

	// Update existing record
	if hasPrimary == true {
		data, err := database.db.Get(primaryKey, nil)
		if err != nil {

			if err == leveldb.ErrNotFound {
				// New record
				return database.UpdateRecord(sequence, primaryKey, nil, projection)
			}

			return err
		}

		// Record exists already
		return database.UpdateRecord(sequence, primaryKey, data, projection)
	}

	return database.UpdateRecord(sequence, primaryKey, nil, projection)
}

func (database *Database) UpdateRecord(sequence uint64, key []byte, origData []byte, updates *Projection) error {

	orig := make(map[string]interface{})

	if origData != nil {
		// Parsing original data
		err := json.Unmarshal(origData, &orig)
		if err != nil {
			return err
		}
	}

	for _, field := range updates.Fields {
		orig[field.Name] = field.Value
	}

	// convert to json
	data, err := json.Marshal(&orig)
	if err != nil {
		return err
	}

	// Write to database
	batch := new(leveldb.Batch)
	batch.Put([]byte("seq"), Uint64ToBytes(sequence))
	batch.Put(key, data)

	return database.db.Write(batch, nil)
}

func (database *Database) DeleteRecord(sequence uint64, key []byte) error {

	// Write to database
	batch := new(leveldb.Batch)
	batch.Put([]byte("seq"), Uint64ToBytes(sequence))
	batch.Delete(key)

	return database.db.Write(batch, nil)
}

func (database *Database) GetSequence() (uint64, error) {

	// Getting create snapshot
	snapshot, err := database.db.GetSnapshot()
	if err != nil {
		return 0, err
	}
	defer snapshot.Release()

	// Getting current sequence number of event
	var seq uint64
	seqData, err := snapshot.Get([]byte("seq"), nil)
	if err != nil {
		log.Warn("Not found seq in database, it will be set zero by default.")
		seq = 0
	} else {
		seq = BytesToUint64(seqData)
	}

	return seq, nil
}

func (database *Database) FetchSnapshot(stream pb.DataSnapshot_GetSnapshotServer) error {

	// Getting create snapshot
	snapshot, err := database.db.GetSnapshot()
	if err != nil {
		return err
	}

	// Getting current sequence number of event
	var seq uint64
	seqData, err := snapshot.Get([]byte("seq"), nil)
	if err != nil {
		log.Warn("Not found seq in database, it will be set zero by default.")
		seq = 0
	} else {
		seq = BytesToUint64(seqData)
	}

	log.WithFields(log.Fields{
		"collection": database.name,
		"seq":        seq,
	}).Info("Client requests data")

	iter := snapshot.NewIterator(util.BytesPrefix([]byte("key-")), nil)

	// Prepare packet
	packet := &pb.SnapshotPacket{
		Collection: database.name,
		Sequence:   seq,
		Entries:    make([]*pb.SnapshotEntry, 0),
	}

	for iter.Next() {

		entry := &pb.SnapshotEntry{
			Data: string(iter.Value()),
		}

		packet.Entries = append(packet.Entries, entry)

		if len(packet.Entries) >= 100 {
			stream.Send(packet)
			packet.Entries = make([]*pb.SnapshotEntry, 0)
		}
	}

	// Send entries if buffer has data still
	if len(packet.Entries) > 0 {
		stream.Send(packet)
	}

	err = iter.Error()
	if err != nil {
		return err
	}

	iter.Release()
	snapshot.Release()

	return nil
}
