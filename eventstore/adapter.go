package eventstore

import (
	"eventstorming"
	"math/rand"
	"strconv"
	"time"

	mgo "gopkg.in/mgo.v2"
)

type MongoAdapterEventStore struct {
	Collection *mgo.Collection
}

func (adapter MongoAdapterEventStore) Save(event *eventstorming.Event) error {
	event.EventID = adapter.generateEventID()
	event.TimeStamp = time.Now().UTC().Format(time.RFC3339)
	return adapter.execute(event)
}

func (adapter MongoAdapterEventStore) generateEventID() string {
	minIDLength := 10000000
	additionalIDLength := 1000000
	rand.Seed(time.Now().UnixNano())
	id := strconv.Itoa(minIDLength + rand.Intn(additionalIDLength))
	return id
}

func (adapter MongoAdapterEventStore) execute(event *eventstorming.Event) error {
	if adapter.Collection.Database.Session.Ping() != nil {
		adapter.Collection.Database.Session.Refresh()
		if err := adapter.Collection.Database.Session.Ping(); err != nil {
			return err
		}
	}
	return adapter.Collection.Insert(event)
}
