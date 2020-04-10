package data_snapshot

import (
	"encoding/json"

	"github.com/nats-io/stan.go"
	"github.com/prometheus/common/log"

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
	EventName string  `json:"event"`
	Table     string  `json:"table"`
	Method    string  `json:"method"`
	Fields    []Field `json:"fields"`
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

		// Getting database for specific table
		db := dm.GetDatabase(projection.Table)
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

	db := service.dbMgr.GetDatabase("users")
	if db == nil {
		return nil
	}

	err := db.FetchSnapshot(stream)
	if err != nil {
		return err
	}

	return nil
}
