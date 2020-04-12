package data_snapshot

import (
	"context"
	"encoding/json"

	"github.com/nats-io/stan.go"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	app "gravity-data-snapshot/app/interface"
	pb "gravity-data-snapshot/pb"
)

type Service struct {
	app   app.AppImpl
	dbMgr *DatabaseManager
}

type Field struct {
	Name    string      `json:"name"`
	Value   interface{} `json:"value"`
	Primary bool        `json:"primary"`
}

type Projection struct {
	EventName  string  `json:"event"`
	Collection string  `json:"collection"`
	Method     string  `json:"method"`
	Fields     []Field `json:"fields"`
}

func CreateService(a app.AppImpl) *Service {

	dm := CreateDatabaseManager()
	if dm == nil {
		return nil
	}

	eb := a.GetEventBus()
	err := eb.On("gravity.store.eventStored", func(msg *stan.Msg) {

		log.Info(string(msg.Data))

		var projection Projection
		err := json.Unmarshal(msg.Data, &projection)
		if err != nil {
			msg.Ack()
			return
		}

		// Getting database for specific collection
		db := dm.GetDatabase(projection.Collection)
		if db == nil {
			return
		}

		msg.Ack()

		err = db.ProcessData(msg.Sequence, &projection)
		if err != nil {
			log.Error(err)
		}
	})

	if err != nil {
		return nil
	}

	// Preparing service
	service := &Service{
		app:   a,
		dbMgr: dm,
	}

	return service
}

func (service *Service) GetSnapshot(in *pb.GetSnapshotRequest, stream pb.DataSnapshot_GetSnapshotServer) error {

	db := service.dbMgr.GetDatabase(in.Collection)
	if db == nil {
		return nil
	}

	err := db.FetchSnapshot(stream)
	if err != nil {
		return err
	}

	return nil
}

func (service *Service) GetSnapshotState(ctx context.Context, in *pb.GetSnapshotStateRequest) (*pb.GetSnapshotStateReply, error) {

	db := service.dbMgr.GetDatabase(in.Collection)
	if db == nil {
		return &pb.GetSnapshotStateReply{}, status.Error(codes.NotFound, "No such collection")
	}

	seq, err := db.GetSequence()
	if err != nil {
		return &pb.GetSnapshotStateReply{}, status.Error(codes.NotFound, "Collection has no data")
	}

	return &pb.GetSnapshotStateReply{
		Collection: in.Collection,
		Sequence:   seq,
	}, nil
}
