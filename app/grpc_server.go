package app

import (
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	app "gravity-data-snapshot/app/interface"
	pb "gravity-data-snapshot/pb"
	data_snapshot "gravity-data-snapshot/services/data_snapshot"
)

func (a *App) InitGRPCServer(host string) error {

	// Start to listen on port
	lis, err := net.Listen("tcp", host)
	if err != nil {
		log.Fatal(err)
		return err
	}

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Starting gRPC server on " + host)

	// Create gRPC server
	s := grpc.NewServer()

	// Register data source adapter service
	dataSnapshotService := data_snapshot.CreateService(app.AppImpl(a))
	pb.RegisterDataSnapshotServer(s, dataSnapshotService)
	reflection.Register(s)

	log.WithFields(log.Fields{
		"service": "DataHandler",
	}).Info("Registered service")

	// Starting server
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}
