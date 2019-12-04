package eventstore

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/bagus212/eventstorming"
	"gopkg.in/mgo.v2"
)

type MongoAdapterEventStore struct {
	URL        string
	DB         string
	Collection string
	Username   string
	Password   string
	collection *mgo.Collection
}

func (adapter *MongoAdapterEventStore) Save(event eventstorming.Event) error {
	event.EventID = adapter.generateEventID()
	event.TimeStamp = time.Now().UTC().Format(time.RFC3339)
	return adapter.execute(event)
}

func (adapter *MongoAdapterEventStore) init() error {
	if adapter.collection == nil {
		if adapter.URL == "" {
			return errors.New("missing url mongo")
		}
		if adapter.DB == "" {
			return errors.New("missing mongo database name")
		}
		session, err := mgo.Dial(adapter.URL)
		if err != nil {
			return err
		}
		if adapter.DB != "" && adapter.Password != "" {
			if err := session.DB(adapter.DB).Login(adapter.Username, adapter.Username); err != nil {
				return err
			}
		}
	}
	return nil
}

func (adapter *MongoAdapterEventStore) generateEventID() string {
	minIDLength := 10000000
	additionalIDLength := 1000000
	rand.Seed(time.Now().UnixNano())
	id := strconv.Itoa(minIDLength + rand.Intn(additionalIDLength))
	return id
}

func (adapter *MongoAdapterEventStore) execute(event eventstorming.Event) error {
	if err := adapter.init(); err != nil {
		return err
	}
	if adapter.collection.Database.Session.Ping() != nil {
		adapter.collection.Database.Session.Refresh()
		if err := adapter.collection.Database.Session.Ping(); err != nil {
			return err
		}
	}
	return adapter.collection.Insert(event)
}
