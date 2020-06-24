package eventbus

import (
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"time"
)

type EventBus struct {
	host       string
	clusterID  string
	clientName string
	client     stan.Conn
}

func CreateConnector(host string, clusterID string, clientName string) *EventBus {
	return &EventBus{
		host:       host,
		clusterID:  clusterID,
		clientName: clientName,
	}
}

func (eb *EventBus) Connect() error {

	log.WithFields(log.Fields{
		"host":       eb.host,
		"clientName": eb.clientName,
		"clusterID":  eb.clusterID,
	}).Info("Connecting to event server")

	// Connect to queue server
	nc, err := nats.Connect(eb.host,
		nats.MaxReconnects(-1),
		nats.PingInterval(10*time.Second),
		nats.MaxPingsOutstanding(3),
	)
	if err != nil {
		return err
	}

	nc.SetReconnectHandler(func(rcb *nats.Conn) {
		log.Info("Reconnecting to eventbus server ...")
	})

	sc, err := stan.Connect(eb.clusterID, eb.clientName, stan.NatsConn(nc))
	if err != nil {
		return err
	}

	eb.client = sc

	return nil
}

func (eb *EventBus) Close() {
	eb.client.Close()
}

func (eb *EventBus) Emit(eventName string, data []byte) error {

	if err := eb.client.Publish(eventName, data); err != nil {
		return err
	}

	return nil
}

func (eb *EventBus) On(eventName string, fn func(*stan.Msg)) error {

	if _, err := eb.client.Subscribe(eventName, fn, stan.SetManualAckMode()); err != nil {
		return err
	}

	return nil
}
