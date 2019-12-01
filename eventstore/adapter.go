package eventstore

import (
	"eventstorming"
	"eventstorming/utils/utilsmongo"
	"math/rand"
	"strconv"
	"time"

	mgo "gopkg.in/mgo.v2"
)

const (
	EVENT_COLL = "eventstore"
)

func NewMongoAdapterEventStore() (MongoAdapterEventStore, error) {
	db, err := utilsmongo.MongoEventStoreLogin()
	if err != nil {
		return MongoAdapterEventStore{}, err
	}
	return MongoAdapterEventStore{
		Collection: db.C(EVENT_COLL),
	}, nil
}

type MongoAdapterEventStore struct {
	Collection *mgo.Collection
}

func (adapter MongoAdapterEventStore) Save(event *eventstorming.Event) error {
	event.EventID = adapter.generateEventID()
	event.TimeStamp = time.Now().UTC().Format(time.RFC3339)
	return adapter.Collection.Insert(event)
}

func (adapter MongoAdapterEventStore) generateEventID() string {
	minIDLength := 10000000
	additionalIDLength := 1000000
	rand.Seed(time.Now().UnixNano())
	id := strconv.Itoa(minIDLength + rand.Intn(additionalIDLength))
	return id
}
