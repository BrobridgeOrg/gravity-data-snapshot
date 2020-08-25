package app

import (
	"fmt"
	"gravity-data-snapshot/app/eventbus"
	app "gravity-data-snapshot/app/interface"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type App struct {
	id       string
	eventbus *eventbus.EventBus
	isReady  bool
}

func CreateApp() *App {

	// Using hostname (pod name) by default
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		return nil
	}

	host = strings.ReplaceAll(host, ".", "_")

	id := fmt.Sprintf("gravity_data_snapshot-%s", host)

	a := &App{
		id: id,
	}

	a.eventbus = eventbus.CreateConnector(
		viper.GetString("event_store.host"),
		viper.GetString("event_store.cluster_id"),
		id,
		func(natsConn *nats.Conn) {

			for {
				log.Warn("re-connect to event server")

				// Connect to NATS Streaming
				err := a.eventbus.Connect()
				if err != nil {
					log.Error("Failed to connect to event server")
					time.Sleep(time.Duration(1) * time.Second)
					continue
				}

				a.isReady = true

				break
			}
		},
		func(natsConn *nats.Conn) {
			a.isReady = false
			log.Error("event server was disconnected")
		},
	)

	return a
}

func (a *App) Init() error {

	log.WithFields(log.Fields{
		"a_id": a.id,
	}).Info("Starting application")

	// Connect to event server
	err := a.eventbus.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (a *App) Uninit() {
}

func (a *App) IsReady() bool {
	return a.isReady
}

func (a *App) Run() error {

	port := strconv.Itoa(viper.GetInt("service.port"))
	err := a.InitGRPCServer(":" + port)
	if err != nil {
		return err
	}

	return nil
}

func (a *App) GetEventBus() app.EventBusImpl {
	return app.EventBusImpl(a.eventbus)
}
