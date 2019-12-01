package utilsmongo

import (
	"gopkg.in/mgo.v2"
)

func New(db *mgo.Database) *MongoCircuitBreaker {
	return &MongoCircuitBreaker{
		Database: db,
	}
}

type MongoCircuitBreaker struct {
	Database *mgo.Database
}

func (cb *MongoCircuitBreaker) Execute(logic func() error) error {
	if cb.Database.Session.Ping() != nil {
		cb.Database.Session.Refresh()
		if err := cb.Database.Session.Ping(); err != nil {
			return err
		}
	}
	return logic()
}
